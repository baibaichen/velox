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

#include "velox/exec/ch/Prefetching.h"

#if defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#elif defined(__linux__)
#include <fstream>
#include <string>
#endif

namespace facebook::velox::exec::ch {
namespace {

constexpr size_t kDefaultL2CacheBytes = 256UL << 10;

#if defined(__x86_64__) || defined(__i386__)
size_t readL2CacheSize() {
  constexpr unsigned kCacheLeaves[] = {0x4U, 0x8000001DU};
  for (const auto leaf : kCacheLeaves) {
    const auto baseLeaf = leaf & 0x80000000U;
    if (__get_cpuid_max(baseLeaf, nullptr) < leaf) {
      continue;
    }
    for (unsigned subleaf = 0; subleaf < 32; ++subleaf) {
      unsigned eax = 0;
      unsigned ebx = 0;
      unsigned ecx = 0;
      unsigned edx = 0;
      __cpuid_count(leaf, subleaf, eax, ebx, ecx, edx);
      const auto cacheType = eax & 0x1FU;
      if (cacheType == 0) {
        break;
      }
      const auto cacheLevel = (eax >> 5) & 0x7U;
      if (cacheLevel != 2 || (cacheType != 1 && cacheType != 3)) {
        continue;
      }
      const auto lineSize = (ebx & 0xFFFU) + 1;
      const auto partitions = ((ebx >> 12) & 0x3FFU) + 1;
      const auto ways = ((ebx >> 22) & 0x3FFU) + 1;
      const auto sets = ecx + 1;
      return static_cast<size_t>(lineSize) * partitions * ways * sets;
    }
  }
  return 0;
}
#elif defined(__linux__)
size_t readL2CacheSize() {
  constexpr const char* kCacheRoot = "/sys/devices/system/cpu/cpu0/cache/index";
  for (size_t index = 0; index < 32; ++index) {
    const auto directory = std::string(kCacheRoot) + std::to_string(index);
    std::ifstream levelFile(directory + "/level");
    size_t level = 0;
    if (!(levelFile >> level)) {
      break;
    }
    if (level != 2) {
      continue;
    }

    std::ifstream typeFile(directory + "/type");
    std::string type;
    std::getline(typeFile, type);
    if (type != "Data" && type != "Unified") {
      continue;
    }

    std::ifstream sizeFile(directory + "/size");
    size_t size = 0;
    char suffix = 0;
    if (!(sizeFile >> size)) {
      continue;
    }
    sizeFile >> suffix;
    if (suffix == 'K' || suffix == 'k') {
      size <<= 10;
    } else if (suffix == 'M' || suffix == 'm') {
      size <<= 20;
    }
    return size;
  }
  return 0;
}
#elif defined(__APPLE__)
size_t readL2CacheSize() {
  uint64_t size = 0;
  auto valueSize = sizeof(size);
  if (sysctlbyname("hw.l2cachesize", &size, &valueSize, nullptr, 0) == 0) {
    return static_cast<size_t>(size);
  }
  return 0;
}
#else
size_t readL2CacheSize() {
  return 0;
}
#endif

const size_t kMinTableBytesForPrefetch = [] {
  const auto detected = readL2CacheSize();
  return detected == 0 ? kDefaultL2CacheBytes : detected;
}();

} // namespace

size_t minTableBytesForPrefetch() {
  return kMinTableBytesForPrefetch;
}

} // namespace facebook::velox::exec::ch
