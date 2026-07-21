#include "velox/ch/Common/ProfileEvents.h"

#include <array>
#include <atomic>
#include <chrono>

namespace facebook::velox::ch
{

namespace ProfileEvents
{

namespace
{
std::array<std::atomic<uint64_t>, kNumEvents> & storage()
{
    static std::array<std::atomic<uint64_t>, kNumEvents> c{};
    return c;
}
} // namespace

void increment(Event e, uint64_t delta)
{
    storage()[static_cast<size_t>(e)].fetch_add(delta, std::memory_order_relaxed);
}

uint64_t get(Event e)
{
    return storage()[static_cast<size_t>(e)].load(std::memory_order_relaxed);
}

} // namespace ProfileEvents

namespace
{
uint64_t nowNs()
{
    // duration_cast guarantees nanoseconds regardless of steady_clock::period.
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
} // namespace

template <typename Unit>
ProfileEventTimeIncrement<Unit>::ProfileEventTimeIncrement(ProfileEvents::Event e)
    : event_(e), startNs_(nowNs())
{
}

template <typename Unit>
ProfileEventTimeIncrement<Unit>::~ProfileEventTimeIncrement()
{
    const uint64_t elapsedNs = nowNs() - startNs_;
    // Convert nanoseconds to microseconds (the only Unit instantiated).
    const uint64_t us = elapsedNs / 1000;
    ProfileEvents::increment(event_, us);
}

template <typename Unit>
uint64_t ProfileEventTimeIncrement<Unit>::elapsed() const
{
    return (nowNs() - startNs_) / 1000;
}

template class ProfileEventTimeIncrement<Microseconds>;

} // namespace facebook::velox::ch
