// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0
// Adapted from .NET Foundation and Contributors; see MemoryCache.LICENSE (MIT).
#pragma once
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/task_queue/LifoSemMPMCQueue.h>
#include <folly/synchronization/Hazptr.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
namespace facebook::velox {
namespace detail {
inline folly::CPUThreadPoolExecutor& memoryCacheExecutor() {
  using Pool = folly::CPUThreadPoolExecutor;
  static Pool pool(
      std::pair<size_t, size_t>{1, 1},
      std::make_unique<folly::LifoSemMPMCQueue<Pool::CPUTask>>(4096));
  return pool;
}
} // namespace detail

template <typename Key, typename Value, template <typename...> class Map>
class MemoryCache {
 public:
  using ValuePtr = std::shared_ptr<Value>;
  using Duration = std::chrono::duration<int64_t, std::ratio<1, 10000000>>;
  using TimePoint =
      std::chrono::time_point<std::chrono::system_clock, Duration>;
  enum class Priority : uint8_t { Low, Normal, High, NeverRemove };
  struct EntryOptions {
    std::optional<TimePoint> absoluteExpiration;
    std::optional<Duration> absoluteExpirationRelativeToNow, slidingExpiration;
    std::optional<int64_t> size;
    Priority priority{Priority::Normal};
  };
  struct Options {
    std::optional<int64_t> sizeLimit;
    Duration expirationScanFrequency{std::chrono::minutes(1)};
    double compactionPercentage{0.05};
    bool trackStatistics{false};
    std::string name{"Default"};
    std::function<TimePoint()> clock;
  };
  struct Statistics {
    uint64_t totalHits, totalMisses, totalEvictions;
    size_t currentEntryCount;
    std::optional<int64_t> currentEstimatedSize;
  };
  explicit MemoryCache(Options options = {})
      : core_(std::make_shared<Core>(std::move(options))) {
    const auto& o = *core_;
    require(
        o.sizeLimit.value_or(0) >= 0 &&
        !(o.compactionPercentage < 0 || o.compactionPercentage > 1));
  }
  ~MemoryCache() noexcept {
    std::lock_guard lock(core_->maintenance);
    core_->closed = true;
  }
  MemoryCache(const MemoryCache&) = delete;
  MemoryCache& operator=(const MemoryCache&) = delete;
  ValuePtr set(const Key& key, ValuePtr value, EntryOptions o = {}) {
    const auto& c = core_;
    require(
        o.size.value_or(0) >= 0 &&
        o.slidingExpiration.value_or(Duration(1)).count() > 0 &&
        o.absoluteExpirationRelativeToNow.value_or(Duration(1)).count() > 0);
    if (c->sizeLimit && !o.size) {
      throw std::logic_error("Size is required");
    }
    if (o.absoluteExpiration) {
      checkTime(o.absoluteExpiration->time_since_epoch().count());
    }
    const auto now = c->now();
    if (o.absoluteExpirationRelativeToNow) {
      const auto ticks = o.absoluteExpirationRelativeToNow->count();
      if (ticks > kMaxTime - now) {
        throw std::out_of_range("Relative expiration exceeds UTC range");
      }
      o.absoluteExpiration = std::min(
          o.absoluteExpiration.value_or(TimePoint::max()),
          TimePoint(Duration(now + ticks)));
    }
    auto entry = std::make_shared<Entry>(key, value, std::move(o), now);
    StatePtr state(c->state);
    auto prior = state->find(key);
    hook("MemoryCache::set::prior", &prior);
    if (prior)
      prior->expire(Reason::Replaced);
    if (entry->expired(now)) {
      c->erase(state, prior);
    } else if (c->reserve(state, entry, prior)) {
      bool added = false;
      auto rollback = folly::makeGuard([&] {
        state->size.fetch_sub(!added && c->sizeLimit ? entry->weight() : 0);
      });
      hook("MemoryCache::set::reserved", &entry);
      if (prior) {
        state->map.visit(key, [&](auto& item) noexcept {
          if (item.second == prior) {
            item.second.swap(entry);
            added = true;
          }
        });
        state->size.fetch_sub(added && c->sizeLimit ? prior->weight() : 0);
      }
      added = added || state->map.try_emplace(key, entry);
      if (!added)
        entry->expire(Reason::Replaced);
    } else {
      entry->expire(Reason::Capacity);
      c->schedule(true, now);
      c->erase(state, prior);
    }
    c->schedule(false, now);
    return value;
  }
  bool tryGetValue(const Key& key, ValuePtr& result) {
    const auto& c = core_;
    StatePtr state(c->state);
    auto entry = state->find(key);
    const auto now = c->now();
    const bool hit =
        entry && (!entry->expired(now) || entry->reason == Reason::Replaced);
    if (hit) {
      entry->accessed = now;
      result = entry->value;
    } else {
      c->erase(state, entry, true);
      result.reset();
    }
    c->schedule(false, now);
    if (c->trackStatistics) {
      ++c->totals[hit ? 0 : 1];
    }
    return hit;
  }
  void remove(const Key& key) {
    StatePtr s(core_->state);
    EntryPtr retired;
    if (s->map.erase_if(key, [&](const auto& item) noexcept {
          retired = item.second;
          return true;
        })) {
      retired->expire(Reason::Removed);
      core_->account(s, retired, false);
    }
    core_->schedule(false, core_->now());
  }
  void clear() {
    auto fresh = std::make_unique<State>();
    fresh->set_cohort_tag(&core_->retired);
    auto old = core_->state.exchange(fresh.release());
    auto retire = folly::makeGuard([&] { old->retire(); });
    hook("MemoryCache::clear::exchanged", &old);
    // Release existing entries outside Map locks, without waiting for readers.
    std::vector<EntryPtr> entries;
    old->map.cvisit_all([&](const auto& item) {
      item.second->expire(Reason::Removed);
      entries.push_back(item.second);
    });
    for (const auto& entry : entries) {
      if (old->map.erase_if(
              entry->key,
              [&](const auto& item) noexcept {
                return item.second == entry;
              }) &&
          core_->sizeLimit) {
        old->size.fetch_sub(entry->weight());
      }
    }
  }
  void compact(double percentage) {
    StatePtr state(core_->state);
    core_->compact(state, amount(state->map.size() * percentage), false);
  }
  size_t count() const {
    return StatePtr(core_->state)->map.size();
  }
  std::vector<Key> keys() const {
    std::vector<Key> result;
    StatePtr(core_->state)->map.cvisit_all([&](const auto& item) {
      hook("MemoryCache::keys::collect", &result);
      result.push_back(item.first);
    });
    return result;
  }
  void report() const noexcept try {
    const auto& c = core_;
    std::lock_guard lock(c->reportMutex);
    auto stats = getCurrentStatistics();
    if (!stats)
      return;
    const char* names[]{
        "hits", "misses", "evictions", "entries", "estimated_size"};
    for (int i = 0; i < (stats->currentEstimatedSize ? 5 : 4); ++i) {
      uint64_t value = i < 3 ? c->totals[i].load()
          : i == 3           ? stats->currentEntryCount
                             : *stats->currentEstimatedSize;
      if (i < 3)
        value -= std::exchange(c->reported[i], value);
      try {
        const auto key = "velox.memory_cache." + c->name + "." + names[i];
        if (!BaseStatsReporter::registered)
          continue;
        if (auto reporter = folly::Singleton<BaseStatsReporter>::try_get()) {
          reporter->registerMetricExportType(
              folly::StringPiece(key), i < 3 ? StatType::SUM : StatType::AVG);
          reporter->addMetricValue(key, value);
        }
      } catch (...) {
      }
    }
  } catch (...) {
  }
  std::optional<Statistics> getCurrentStatistics() const {
    const auto& c = core_;
    if (!c->trackStatistics) {
      return std::nullopt;
    }
    StatePtr s(c->state);
    return Statistics{
        c->totals[0],
        c->totals[1],
        c->totals[2],
        s->map.size(),
        c->sizeLimit ? std::optional<int64_t>(s->size.load()) : std::nullopt};
  }

 private:
  friend struct MemoryCacheTestAccess;
  enum class Reason { None, Removed, Replaced, Expired, Capacity };
  static constexpr int64_t kMinTime = -621355968000000000LL,
                           kMaxTime = 2534023007999999999LL;
  static void require(bool valid) {
    if (!valid)
      throw std::invalid_argument("Invalid MemoryCache options");
  }
  static void checkTime(int64_t time) {
    if (time < kMinTime || time > kMaxTime) {
      throw std::out_of_range("Time outside .NET UTC range");
    }
  }
  static int64_t amount(double value) {
    if (!(value > 0))
      return 0;
    return value >= double(INT64_MAX) ? INT64_MAX : int64_t(value);
  }
  static void hook(std::string_view name, void* data) {
    common::testutil::TestValue::adjust(name, data);
  }
  struct Entry : EntryOptions {
    Key key;
    ValuePtr value;
    std::atomic<int64_t> accessed;
    std::atomic<Reason> reason{Reason::None};
    Entry(const Key& k, ValuePtr v, EntryOptions o, int64_t now)
        : EntryOptions(std::move(o)),
          key(k),
          value(std::move(v)),
          accessed(now) {}
    uint64_t weight() const {
      return this->size.value_or(0);
    }
    void expire(Reason why) {
      auto expected = Reason::None;
      reason.compare_exchange_strong(expected, why);
    }
    bool expired(int64_t now) {
      if ((this->absoluteExpiration &&
           now >= this->absoluteExpiration->time_since_epoch().count()) ||
          (this->slidingExpiration &&
           now - accessed.load() >= this->slidingExpiration->count())) {
        expire(Reason::Expired);
      }
      return reason != Reason::None;
    }
  };
  using EntryPtr = std::shared_ptr<Entry>;
  struct State : folly::hazptr_obj_base<State> {
    Map<Key, EntryPtr> map;
    std::atomic<uint64_t> size{0};
    EntryPtr find(const Key& key) {
      EntryPtr result;
      map.cvisit(key, [&](const auto& item) { result = item.second; });
      return result;
    }
  };
  struct StatePtr {
    folly::hazptr_holder<> guard{folly::make_hazard_pointer()};
    State* ptr;
    explicit StatePtr(const std::atomic<State*>& source)
        : ptr(guard.protect(source)) {}
    State* operator->() const {
      return ptr;
    }
  };
  struct Core : Options, std::enable_shared_from_this<Core> {
    // Core outlives every holder: foreground calls precede destruction and
    // accepted maintenance tasks own Core. Reclaim retired states at teardown.
    folly::hazptr_obj_cohort<> retired;
    std::atomic<State*> state{nullptr};
    std::atomic<uint64_t> totals[3]{};
    std::mutex maintenance, reportMutex;
    uint64_t reported[3]{};
    bool closed{false}, busy[2]{}, rescan{false};
    std::atomic<int64_t> lastScan;
    explicit Core(Options o) : Options(std::move(o)), lastScan(now()) {
      auto initial = std::make_unique<State>();
      initial->set_cohort_tag(&retired);
      state = initial.release();
    }
    ~Core() {
      delete state.load();
    }
    int64_t now() const {
      const auto time = (this->clock ? this->clock()
                                     : std::chrono::time_point_cast<Duration>(
                                           std::chrono::system_clock::now()))
                            .time_since_epoch()
                            .count();
      checkTime(time);
      return time;
    }
    bool reserve(const StatePtr& s, const EntryPtr& e, const EntryPtr& old) {
      if (!this->sizeLimit)
        return true;
      auto size = s->size.load();
      for (int i = 0; i < 100; ++i) {
        if (__uint128_t(size) + e->weight() - (old ? old->weight() : 0) >
            uint64_t(*this->sizeLimit))
          return false;
        if (s->size.compare_exchange_weak(size, size + e->weight()))
          return true;
      }
      return false;
    }
    void account(const StatePtr& s, const EntryPtr& e, bool eviction) {
      s->size.fetch_sub(this->sizeLimit ? e->weight() : 0);
      totals[2] += eviction && this->trackStatistics;
    }
    void erase(const StatePtr& s, const EntryPtr& e, bool eviction = false) {
      if (e && s->map.erase_if(e->key, [&](const auto& item) noexcept {
            return item.second == e;
          })) {
        account(s, e, eviction);
      }
    }
    void compact(const StatePtr& s, int64_t target, bool weighted) {
      using Candidate = std::tuple<int, int64_t, EntryPtr>;
      std::vector<Candidate> candidates;
      const auto time = now();
      s->map.cvisit_all([&](const auto& item) {
        auto e = item.second;
        const int priority = e->expired(time) ? -1 : int(e->priority);
        require(target < 0 || priority <= int(Priority::NeverRemove));
        if (priority < (target < 0 ? 0 : int(Priority::NeverRemove))) {
          candidates.emplace_back(priority, e->accessed.load(), e);
        }
      });
      std::sort(candidates.begin(), candidates.end());
      size_t selected = 0;
      for (auto& [priority, time, e] : candidates) {
        if (priority >= 0 && target <= 0)
          break;
        e->expire(Reason::Capacity);
        ++selected;
        target =
            std::max<int64_t>(0, target - int64_t(weighted ? e->weight() : 1));
      }
      hook("MemoryCache::compact::candidates", &candidates);
      for (size_t i = 0; i < selected; ++i)
        erase(s, std::get<2>(candidates[i]), true);
    }
    void schedule(bool capacity, int64_t time) {
      if (!capacity &&
          time - lastScan.load() <= this->expirationScanFrequency.count())
        return;
      std::lock_guard lock(maintenance);
      if (closed ||
          (!capacity &&
           time - lastScan <= this->expirationScanFrequency.count()))
        return;
      if (busy[capacity]) {
        rescan |= !capacity;
        return;
      }
      auto& executor = detail::memoryCacheExecutor();
      auto self = this->shared_from_this();
      executor.add(
          [self, capacity, keep = folly::getKeepAliveToken(&executor)] {
            for (;;) {
              try {
                hook("MemoryCache::maintenance::start", self.get());
                StatePtr s(self->state);
                int64_t target = -1;
                if (capacity) {
                  const auto low = *self->sizeLimit -
                      amount(*self->sizeLimit * self->compactionPercentage);
                  target = std::max<int64_t>(0, s->size.load() - low);
                } else
                  self->lastScan = self->now();
                if (!capacity || target > 0)
                  self->compact(s, target, capacity);
              } catch (...) {
              }
              std::lock_guard lock(self->maintenance);
              if (!capacity && std::exchange(self->rescan, false))
                continue;
              self->busy[capacity] = false;
              return;
            }
          });
      busy[capacity] = true;
      if (!capacity)
        lastScan = time;
    }
  };
  const std::shared_ptr<Core> core_;
};
} // namespace facebook::velox
