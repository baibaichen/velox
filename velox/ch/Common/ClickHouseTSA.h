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

/// Macros for Clang Thread Safety Analysis (TSA). They can be safely ignored by other compilers.
/// Feel free to extend, but please stay close to https://clang.llvm.org/docs/ThreadSafetyAnalysis.html#mutexheader
///
/// These definitions preserve the ClickHouse lock-to-data thread-safety contract:
/// Clang honors the annotations under `-Wthread-safety`, while other compilers
/// treat them as compile-time no-ops. GCC rejects the unknown `__attribute__`
/// names under `-Werror=attributes`, so they must only expand under Clang.
#if defined(__clang__)
#define TSA_ATTRIBUTE(...) __attribute__((__VA_ARGS__))
#else
#define TSA_ATTRIBUTE(...)
#endif

#define TSA_GUARDED_BY(...) TSA_ATTRIBUTE(guarded_by(__VA_ARGS__))                       /// data is protected by given capability
#define TSA_PT_GUARDED_BY(...) TSA_ATTRIBUTE(pt_guarded_by(__VA_ARGS__))                 /// pointed-to data is protected by the given capability
#define TSA_REQUIRES(...) TSA_ATTRIBUTE(requires_capability(__VA_ARGS__))                /// thread needs exclusive possession of given capability
#define TSA_REQUIRES_SHARED(...) TSA_ATTRIBUTE(requires_shared_capability(__VA_ARGS__))  /// thread needs shared possession of given capability
#define TSA_ACQUIRED_AFTER(...) TSA_ATTRIBUTE(acquired_after(__VA_ARGS__))               /// annotated lock must be locked after given lock
#define TSA_NO_THREAD_SAFETY_ANALYSIS TSA_ATTRIBUTE(no_thread_safety_analysis)           /// disable TSA for a function
#define TSA_CAPABILITY(...) TSA_ATTRIBUTE(capability(__VA_ARGS__))                       /// object of a class can be used as capability
#define TSA_ACQUIRE(...) TSA_ATTRIBUTE(acquire_capability(__VA_ARGS__))                        /// function acquires a capability, but does not release it
#define TSA_TRY_ACQUIRE(...) TSA_ATTRIBUTE(try_acquire_capability(__VA_ARGS__))                /// function tries to acquire a capability and returns a boolean value indicating success or failure
#define TSA_RELEASE(...) TSA_ATTRIBUTE(release_capability(__VA_ARGS__))                        /// function releases the given capability
#define TSA_ACQUIRE_SHARED(...) TSA_ATTRIBUTE(acquire_shared_capability(__VA_ARGS__))          /// function acquires a shared capability, but does not release it
#define TSA_TRY_ACQUIRE_SHARED(...) TSA_ATTRIBUTE(try_acquire_shared_capability(__VA_ARGS__))  /// function tries to acquire a shared capability and returns a boolean value indicating success or failure
#define TSA_RELEASE_SHARED(...) TSA_ATTRIBUTE(release_shared_capability(__VA_ARGS__))          /// function releases the given shared capability
#define TSA_SCOPED_LOCKABLE TSA_ATTRIBUTE(scoped_lockable) /// object of a class has scoped lockable capability
#define TSA_RETURN_CAPABILITY(...) TSA_ATTRIBUTE(lock_returned(__VA_ARGS__))             /// to return capabilities in functions

/// Macros for suppressing TSA warnings for specific reads/writes (instead of suppressing it for the whole function)
/// They use a lambda function to apply function attribute to a single statement. This enable us to suppress warnings locally instead of
/// suppressing them in the whole function
/// Consider adding a comment when using these macros.
#define TSA_SUPPRESS_WARNING_FOR_READ(x) ([&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> const auto & { return (x); }())
#define TSA_SUPPRESS_WARNING_FOR_WRITE(x) ([&]() TSA_NO_THREAD_SAFETY_ANALYSIS -> auto & { return (x); }())

/// This macro is useful when only one thread writes to a member
/// and you want to read this member from the same thread without locking a mutex.
/// It's safe (because no concurrent writes are possible), but TSA generates a warning.
/// (Seems like there's no way to verify it, but it makes sense to distinguish it from TSA_SUPPRESS_WARNING_FOR_READ for readability)
#define TSA_READ_ONE_THREAD(x) TSA_SUPPRESS_WARNING_FOR_READ(x)
