#include "velox/ch/Common/CurrentMetrics.h"

#include <array>
#include <atomic>

namespace facebook::velox::ch
{

namespace CurrentMetrics
{

namespace
{
std::array<std::atomic<int64_t>, kNumMetrics> & storage()
{
    static std::array<std::atomic<int64_t>, kNumMetrics> v{};
    return v;
}
} // namespace

void add(Metric m, int64_t delta)
{
    storage()[static_cast<size_t>(m)].fetch_add(delta, std::memory_order_relaxed);
}

void sub(Metric m, int64_t delta)
{
    storage()[static_cast<size_t>(m)].fetch_sub(delta, std::memory_order_relaxed);
}

int64_t get(Metric m)
{
    return storage()[static_cast<size_t>(m)].load(std::memory_order_relaxed);
}

void set(Metric m, int64_t v)
{
    storage()[static_cast<size_t>(m)].store(v, std::memory_order_relaxed);
}

Increment::Increment(Metric m, int64_t delta) : metric_(m), delta_(delta)
{
    add(metric_, delta_);
}

Increment::~Increment()
{
    sub(metric_, delta_);
}

} // namespace CurrentMetrics

} // namespace facebook::velox::ch
