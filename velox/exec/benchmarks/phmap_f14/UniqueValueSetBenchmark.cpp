/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <array>
#include <iostream>
#include <numeric>
#include <random>
#include <stdexcept>

#include "velox/exec/VectorHasher.h"

DEFINE_string(
    uv_kind,
    "i64_random",
    "i64_seq, i64_random, str8, str24, str128");
DEFINE_uint32(uv_size, 4096, "Distinct values (2..100000)");
DEFINE_bool(uv_reverse, false, "Register phmap before F14");
DEFINE_bool(
    uv_memory,
    false,
    "Emit allocation measurements instead of timings");
DEFINE_bool(
    uv_check_only,
    false,
    "Run correctness checks without benchmarking");

namespace {
using facebook::velox::exec::UniqueValue;
using facebook::velox::exec::UniqueValueComparer;
using facebook::velox::exec::UniqueValueHasher;

template <typename Alloc = std::allocator<UniqueValue>>
using F14 = folly::
    F14FastSet<UniqueValue, UniqueValueHasher, UniqueValueComparer, Alloc>;
template <typename Alloc = std::allocator<UniqueValue>>
using Phmap = phmap::
    flat_hash_set<UniqueValue, UniqueValueHasher, UniqueValueComparer, Alloc>;

static_assert(sizeof(UniqueValue) == 16);
static_assert(FOLLY_F14_VECTOR_INTRINSICS_AVAILABLE);
static_assert(
    std::is_base_of_v<
        folly::F14ValueSet<UniqueValue, UniqueValueHasher, UniqueValueComparer>,
        F14<>>);

void check(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

uint64_t mix(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

struct Queries {
  std::vector<UniqueValue> hit;
  std::vector<UniqueValue> miss;
  std::vector<UniqueValue> half;
  std::vector<UniqueValue> mixedInsert;
};

struct Data {
  size_t size;
  size_t prefixSize;
  size_t stringLength{0};
  std::vector<std::string> strings;
  std::vector<std::string> probeStrings;
  std::vector<UniqueValue> build;
  std::array<Queries, 2> queries;

  Data(const std::string& kind, size_t count)
      : size(count), prefixSize(count - std::max<size_t>(1, count / 10)) {
    check(count >= 2 && count <= 100000, "uv_size must be 2..100000");
    check(
        kind == "i64_seq" || kind == "i64_random" || kind == "str8" ||
            kind == "str24" || kind == "str128",
        "Invalid uv_kind");
    if (kind.starts_with("str")) {
      stringLength = std::stoul(kind.substr(3));
      strings.resize(2 * size);
      for (size_t i = 0; i < strings.size(); ++i) {
        auto& str = strings[i];
        str.resize(stringLength);
        for (size_t j = 0; j < stringLength - 8; ++j) {
          str[j] = 'a' + mix(i * 131 + j) % 26;
        }
        for (size_t j = 0; j < 8; ++j) {
          str[stringLength - 1 - j] = "0123456789abcdef"[(i >> (j * 4)) & 15];
        }
      }
      probeStrings = strings;
    }
    auto value = [&](size_t index, bool probe, uint32_t id) {
      UniqueValue result = stringLength
          ? UniqueValue(
                (probe ? probeStrings : strings)[index].data(), stringLength)
          : UniqueValue(kind == "i64_seq" ? index : mix(index));
      result.setId(id);
      return result;
    };
    std::mt19937_64 rng(20261109);
    std::vector<size_t> order(size);
    std::iota(order.begin(), order.end(), 0);
    std::shuffle(order.begin(), order.end(), rng);
    for (size_t i = 0; i < size; ++i) {
      build.push_back(value(order[i], false, i + 1));
    }
    for (size_t hot = 0; hot < queries.size(); ++hot) {
      auto& q = queries[hot];
      const auto length = std::max<size_t>(4096, size);
      const auto hotSize = std::max<size_t>(1, size / 100);
      for (size_t i = 0; i < length; ++i) {
        const auto pos = hot && i % 10 != 0 ? rng() % hotSize : i % size;
        q.hit.push_back(value(order[pos], true, pos + 1));
        q.miss.push_back(value(size + order[pos], true, 0));
        q.half.push_back(i % 2 ? q.hit.back() : q.miss.back());
      }
      std::shuffle(q.hit.begin(), q.hit.end(), rng);
      std::shuffle(q.miss.begin(), q.miss.end(), rng);
      std::shuffle(q.half.begin(), q.half.end(), rng);
      // Exactly one new key in every ten operations; existing keys are from the
      // prefix.
      for (size_t i = 0; i < (size - prefixSize) * 10; ++i) {
        const auto pos = i % 10 == 0 ? prefixSize + i / 10
                                     : rng() %
                (hot && i % 10 != 9 ? std::min(hotSize, prefixSize)
                                    : prefixSize);
        q.mixedInsert.push_back(value(order[pos], true, pos + 1));
      }
    }
  }
};

template <typename Set>
void fill(Set& set, const Data& data, size_t count) {
  for (size_t i = 0; i < count; ++i) {
    auto value = data.build[i];
    value.setId(set.size() + 1);
    set.insert(value);
  }
}

template <typename Set>
uint64_t insertBatch(Set& set, const std::vector<UniqueValue>& values) {
  uint64_t sum = 0;
  for (auto value : values) {
    value.setId(set.size() + 1);
    auto [it, inserted] = set.insert(value);
    sum += it->id() + inserted;
  }
  return sum;
}

template <typename Set>
uint64_t findBatch(const Set& set, const std::vector<UniqueValue>& values) {
  uint64_t sum = 0;
  for (const auto& value : values) {
    const auto it = set.find(value);
    sum += it == set.end() ? 0 : it->id();
  }
  return sum;
}

template <typename Set>
void verify(const Data& data) {
  for (bool reserve : {false, true}) {
    Set set;
    if (reserve) {
      set.reserve(data.size);
    }
    for (auto value : data.build) {
      const auto expected = value.id();
      value.setId(set.size() + 1);
      const auto [it, inserted] = set.insert(value);
      check(inserted && it->id() == expected, "New value / ID mismatch");
    }
    check(set.size() == data.size, "Distinct count mismatch");
    for (const auto& q : data.queries) {
      for (const auto* input : {&q.hit, &q.miss, &q.half}) {
        for (const auto& value : *input) {
          auto it = set.find(value);
          check(
              value.id() ? it != set.end() && it->id() == value.id()
                         : it == set.end(),
              "Lookup mismatch");
        }
      }
      for (auto value : q.hit) {
        const auto expected = value.id();
        value.setId(set.size() + 1);
        auto [it, inserted] = set.insert(value);
        check(!inserted && it->id() == expected, "Duplicate changed ID");
      }
      Set mixed;
      fill(mixed, data, data.prefixSize);
      for (auto value : q.mixedInsert) {
        const auto expected = value.id();
        value.setId(mixed.size() + 1);
        auto [it, inserted] = mixed.insert(value);
        check(
            inserted == (expected > data.prefixSize) && it->id() == expected,
            "Mixed insertion mismatch");
      }
      check(mixed.size() == data.size, "Mixed distinct count mismatch");
    }
  }
}

template <typename Set>
void verifyStringEdges() {
  const std::vector<std::string> strings{
      "",
      std::string(1, '\0'),
      "a",
      std::string("a\0", 2),
      std::string(7, 'a'),
      std::string(8, 'a'),
      std::string(9, 'a'),
      std::string(15, 'a'),
      std::string(16, 'a'),
      std::string(17, 'a')};
  const auto probes = strings;
  Set set;
  for (size_t i = 0; i < strings.size(); ++i) {
    UniqueValue value(strings[i].data(), strings[i].size());
    value.setId(i + 1);
    check(set.insert(value).second, "Different string lengths were conflated");
  }
  for (size_t i = 0; i < probes.size(); ++i) {
    UniqueValue value(probes[i].data(), probes[i].size());
    value.setId(999);
    auto [it, inserted] = set.insert(value);
    check(
        !inserted && it->id() == i + 1, "String edge / duplicate ID mismatch");
  }
}

enum class Operation { Grow, Reserved, Duplicate, Mixed, Find };

template <typename Set>
unsigned measure(
    unsigned requested,
    const Data& data,
    const std::vector<UniqueValue>& input,
    Operation op) {
  folly::BenchmarkSuspender pause;
  const unsigned batches = (requested - 1) / input.size() + 1;
  if (op == Operation::Grow || op == Operation::Reserved ||
      op == Operation::Mixed) {
    for (unsigned i = 0; i < batches; ++i) {
      Set set;
      if (op == Operation::Reserved) {
        set.reserve(data.size);
      } else if (op == Operation::Mixed) {
        fill(set, data, data.prefixSize);
      }
      pause.dismiss();
      auto sum = insertBatch(set, input);
      folly::doNotOptimizeAway(sum);
      pause.rehire();
    }
  } else {
    Set set;
    fill(set, data, data.size);
    pause.dismiss();
    if (op == Operation::Find) {
      for (unsigned i = 0; i < batches; ++i) {
        // The container overload also clobbers memory, preventing batch
        // hoisting.
        folly::doNotOptimizeAway(set);
        auto sum = findBatch(set, input);
        folly::doNotOptimizeAway(sum);
      }
    } else {
      for (unsigned i = 0; i < batches; ++i) {
        auto sum = insertBatch(set, input);
        folly::doNotOptimizeAway(sum);
      }
    }
    pause.rehire();
  }
  return batches * input.size();
}

template <typename Set>
void registerCases(const std::string& name, const Data& data) {
  auto add = [&](const std::string& label,
                 const std::vector<UniqueValue>& values,
                 Operation op) {
    folly::addBenchmark(__FILE__, name + "/" + label, [&, op](unsigned n) {
      return measure<Set>(n, data, values, op);
    });
  };
  add("grow", data.build, Operation::Grow);
  add("reserved", data.build, Operation::Reserved);
  for (size_t hot = 0; hot < data.queries.size(); ++hot) {
    const auto suffix = hot ? "/hot" : "/uniform";
    const auto& q = data.queries[hot];
    add(std::string("duplicate") + suffix, q.hit, Operation::Duplicate);
    add(std::string("mixed_insert") + suffix, q.mixedInsert, Operation::Mixed);
    add(std::string("find_hit") + suffix, q.hit, Operation::Find);
    add(std::string("find_miss") + suffix, q.miss, Operation::Find);
    add(std::string("find_half") + suffix, q.half, Operation::Find);
  }
}

struct Allocations {
  size_t live{0};
  size_t peak{0};
  size_t calls{0};
};

template <typename T>
struct CountingAllocator {
  using value_type = T;
  Allocations* stats;

  explicit CountingAllocator(Allocations* stats) : stats(stats) {}
  template <typename U>
  CountingAllocator(const CountingAllocator<U>& other) : stats(other.stats) {}
  T* allocate(size_t n) {
    auto* p = std::allocator<T>{}.allocate(n);
    stats->live += n * sizeof(T);
    stats->peak = std::max(stats->peak, stats->live);
    ++stats->calls;
    return p;
  }
  void deallocate(T* p, size_t n) {
    stats->live -= n * sizeof(T);
    std::allocator<T>{}.deallocate(p, n);
  }
  template <typename U>
  bool operator==(const CountingAllocator<U>& other) const {
    return stats == other.stats;
  }
};

template <template <typename> class Container>
void memory(const char* name, const Data& data, bool reserve) {
  Allocations stats;
  {
    Container<CountingAllocator<UniqueValue>> set{
        CountingAllocator<UniqueValue>(&stats)};
    if (reserve) {
      set.reserve(data.size);
    }
    fill(set, data, data.size);
    check(set.size() == data.size, "Memory run distinct count mismatch");
    Container<std::allocator<UniqueValue>> baseline;
    if (reserve) {
      baseline.reserve(data.size);
    }
    fill(baseline, data, data.size);
    check(
        set.bucket_count() == baseline.bucket_count(),
        "Counting allocator changed capacity");
    if constexpr (requires { baseline.getAllocatedMemorySize(); }) {
      check(
          stats.live == baseline.getAllocatedMemorySize(),
          "F14 byte accounting mismatch");
    }
    std::cout << name << ',' << FLAGS_uv_kind << ',' << data.size << ','
              << (reserve ? "reserved" : "grow") << ','
              << sizeof(Container<std::allocator<UniqueValue>>) << ','
              << stats.live << ',' << stats.peak << ',' << stats.calls << ','
              << (data.stringLength > 8 ? data.stringLength * data.size : 0)
              << '\n';
  }
  check(stats.live == 0, "Allocation accounting did not return to zero");
}
} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  try {
    Data data(FLAGS_uv_kind, FLAGS_uv_size);
    verifyStringEdges<F14<>>();
    verifyStringEdges<Phmap<>>();
    verify<F14<>>(data);
    verify<Phmap<>>(data);
    std::cerr << "Verified " << FLAGS_uv_kind << '/' << FLAGS_uv_size << '\n';
    if (FLAGS_uv_check_only) {
      return 0;
    }
    if (FLAGS_uv_memory) {
      std::cout
          << "container,kind,ndv,mode,object_bytes,live_bytes,peak_bytes,allocations,string_payload_bytes\n";
      for (bool reserve : {false, true}) {
        memory<F14>("F14", data, reserve);
        memory<Phmap>("phmap", data, reserve);
      }
      return 0;
    }
    if (FLAGS_uv_reverse) {
      registerCases<Phmap<>>("phmap", data);
      registerCases<F14<>>("F14", data);
    } else {
      registerCases<F14<>>("F14", data);
      registerCases<Phmap<>>("phmap", data);
    }
    folly::runBenchmarks();
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
