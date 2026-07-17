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
#include <deque>
#include <mutex>
#include <type_traits>
#include <utility>

namespace facebook::velox::ch
{

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

    bool push(T value)
    {
        {
            std::unique_lock lock(mutex_);
            producerCv_.wait(lock, [&]
            {
                return finished_ || queue_.size() < capacity_;
            });

            if (finished_)
                return false;

            queue_.emplace_back(std::move(value));
        }

        consumerCv_.notify_one();
        return true;
    }

    bool tryPush(T value)
    {
        {
            std::lock_guard lock(mutex_);
            if (finished_ || queue_.size() >= capacity_)
                return false;
            queue_.emplace_back(std::move(value));
        }

        consumerCv_.notify_one();
        return true;
    }

    bool pop(T & value)
    {
        {
            std::unique_lock lock(mutex_);
            consumerCv_.wait(lock, [&]
            {
                return finished_ || !queue_.empty();
            });

            if (finished_ && queue_.empty())
                return false;

            if constexpr (
                std::is_nothrow_move_assignable_v<T>
                || !std::is_copy_assignable_v<T>)
                value = std::move(queue_.front());
            else
                value = queue_.front();

            queue_.pop_front();
        }

        producerCv_.notify_one();
        return true;
    }

    void finish()
    {
        {
            std::lock_guard lock(mutex_);
            finished_ = true;
        }

        producerCv_.notify_all();
        consumerCv_.notify_all();
    }

    bool isFinished() const
    {
        std::lock_guard lock(mutex_);
        return finished_;
    }

    size_t size() const
    {
        std::lock_guard lock(mutex_);
        return queue_.size();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable producerCv_;
    std::condition_variable consumerCv_;
    std::deque<T> queue_;
    size_t capacity_;
    bool finished_ = false;
};

}
