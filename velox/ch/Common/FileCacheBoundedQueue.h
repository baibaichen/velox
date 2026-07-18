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
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <chrono>
#include <deque>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

namespace facebook::velox::ch
{

// Mirrors ClickHouse's src/Common/ConcurrentBoundedQueue.h: a fixed-capacity,
// thread-safe queue that can be permanently "finished". Producers block until
// capacity is available or the queue is finished; consumers block until data
// is available or the queue is finished and drained.
template <typename T>
class FileCacheBoundedQueue
{
public:
    explicit FileCacheBoundedQueue(size_t capacity)
        : capacity_(capacity)
    {
    }

    FileCacheBoundedQueue(const FileCacheBoundedQueue &) = delete;
    FileCacheBoundedQueue & operator=(const FileCacheBoundedQueue &) = delete;

    // Blocks until capacity is available or the queue is finished. Returns
    // false if the queue was (or becomes) finished before capacity frees up.
    bool push(T value)
    {
        return emplaceImpl(std::nullopt, std::move(value));
    }

    // Never blocks longer than timeoutMilliseconds (0 means non-blocking).
    // Returns false if the queue is full, finished, or the timeout elapses.
    bool tryPush(const T & value, uint64_t timeoutMilliseconds = 0)
    {
        return emplaceImpl(timeoutMilliseconds, value);
    }

    bool tryPush(T && value, uint64_t timeoutMilliseconds = 0)
    {
        return emplaceImpl(timeoutMilliseconds, std::move(value));
    }

    // Blocks until data is available or the queue is finished. After finish,
    // still returns queued values in FIFO order before finally returning
    // false once the queue is empty.
    bool pop(T & value)
    {
        return popImpl(value, std::nullopt);
    }

    // Never blocks. Returns false if the queue is currently empty.
    bool tryPop(T & value)
    {
        {
            std::lock_guard lock(mutex_);
            if (queue_.empty())
                return false;

            assignFront(value);
            queue_.pop_front();
        }

        producerCv_.notify_one();
        return true;
    }

    // Never blocks longer than timeoutMilliseconds. Returns false if the
    // queue stays empty for the whole timeout, or is finished and drained.
    bool tryPop(T & value, uint64_t timeoutMilliseconds)
    {
        return popImpl(value, timeoutMilliseconds);
    }

    // Idempotently marks the queue as finished: subsequent pushes fail, and
    // every blocked producer and consumer wakes up.
    void finish()
    {
        {
            std::lock_guard lock(mutex_);
            finished_ = true;
        }

        producerCv_.notify_all();
        consumerCv_.notify_all();
    }

private:
    template <typename U>
    bool emplaceImpl(std::optional<uint64_t> timeoutMilliseconds, U && value)
    {
        {
            std::unique_lock lock(mutex_);
            auto predicate = [&]
            {
                return finished_ || queue_.size() < capacity_;
            };

            if (timeoutMilliseconds.has_value())
            {
                if (!producerCv_.wait_for(
                        lock,
                        std::chrono::milliseconds(*timeoutMilliseconds),
                        predicate))
                    return false;
            }
            else
            {
                producerCv_.wait(lock, predicate);
            }

            if (finished_)
                return false;

            queue_.emplace_back(std::forward<U>(value));
        }

        consumerCv_.notify_one();
        return true;
    }

    bool popImpl(T & value, std::optional<uint64_t> timeoutMilliseconds)
    {
        {
            std::unique_lock lock(mutex_);
            auto predicate = [&]
            {
                return finished_ || !queue_.empty();
            };

            if (timeoutMilliseconds.has_value())
            {
                if (!consumerCv_.wait_for(
                        lock,
                        std::chrono::milliseconds(*timeoutMilliseconds),
                        predicate))
                    return false;
            }
            else
            {
                consumerCv_.wait(lock, predicate);
            }

            if (finished_ && queue_.empty())
                return false;

            assignFront(value);
            queue_.pop_front();
        }

        producerCv_.notify_one();
        return true;
    }

    // Moves the front element into value only when the move-assignment
    // cannot throw; otherwise copies, so that a throwing copy leaves the
    // front element queued and recoverable (popped again later) rather than
    // losing it to a partially-completed, potentially-throwing move.
    void assignFront(T & value)
    {
        if constexpr (std::is_nothrow_move_assignable_v<T>)
            value = std::move(queue_.front());
        else
            value = queue_.front();
    }

    mutable std::mutex mutex_;
    std::condition_variable producerCv_;
    std::condition_variable consumerCv_;
    std::deque<T> queue_;
    size_t capacity_;
    bool finished_ = false;
};

}
