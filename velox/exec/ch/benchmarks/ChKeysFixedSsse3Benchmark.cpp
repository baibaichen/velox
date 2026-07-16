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

// ============================================================================
// ch-task7 (O5) 多机 A/B micro-benchmark:同一二进制切 SSSE3 packFixedShuffle
// vs 标量 packFixed 两条路径,压测复合定长 key 的宽 key pack 吞吐。
//
// 用途:SSSE3 收益跟微架构强相关,单机结论不通用 —— 把这个二进制拷到不同机器
// 上跑,对比两路径吞吐。两路径 pack 出的宽 key 逐字节相同(测里已对拍),这里只
// 比速度。
//
// 怎么跑(同一二进制):
//   源 env:  source /root/oss/velox-help/env.sh
//   编:      /root/oss/velox-help/build.sh velox_exec_ch_keysfixed_ssse3_benchmark
//   跑:      cd _build/debug/velox/exec/ch/benchmarks
//            ./velox_exec_ch_keysfixed_ssse3_benchmark
//   两条 folly benchmark(scalar / ssse3)在同一次运行里都跑,直接看相对时间。
//   ARM / 无 SSSE3 编译:ssse3 路径自动回退标量(packRowSsse3 内 #if 守卫),
//   两条曲线会一样 —— 说明该机器无 SSSE3 加速。
// ============================================================================

#include "velox/exec/ch/Common/Arena.h"
#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h"
#include "velox/exec/ch/Common/HashTable/HashMap.h"
#include "velox/exec/ch/Common/ColumnsHashing/HashMethod.h"

#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <cstdint>
#include <random>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

using ch::UInt128;
using Map128 = ch::HashMapAll_keys128;
using Method128 = HashMethodKeysFixed<
    Map128::value_type,
    UInt128,
    Map128::mapped_type,
    /*has_nullable_keys_=*/false,
    /*has_low_cardinality_=*/false,
    /*use_cache=*/false>;

// 复合定长 key 档:bigint(8)+int(4)+smallint(2)+tinyint(1) = 15B -> UInt128。
// 走 SSSE3 路径(sizes 非全 {1,2,4,8,16} 也 OK,A/B 入口绕开 prepared)。
struct Fixture {
  static constexpr size_t kRows = 1u << 16; // 65536 行
  std::shared_ptr<memory::MemoryPool> pool;
  VectorPtr v0, v1, v2, v3;
  std::unique_ptr<Method128> method;

  Fixture() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options o;
      memory::MemoryManager::initialize(o);
    }
    pool = memory::memoryManager()->addLeafPool("ch-ssse3-bench");

    std::mt19937_64 rng(12345);
    std::vector<int64_t> c0(kRows);
    std::vector<int32_t> c1(kRows);
    std::vector<int16_t> c2(kRows);
    std::vector<int8_t> c3(kRows);
    for (size_t i = 0; i < kRows; ++i) {
      c0[i] = static_cast<int64_t>(rng());
      c1[i] = static_cast<int32_t>(rng());
      c2[i] = static_cast<int16_t>(rng());
      c3[i] = static_cast<int8_t>(rng());
    }
    auto flat = [&](auto& data) {
      using T = typename std::decay_t<decltype(data)>::value_type;
      auto vec = BaseVector::create<FlatVector<T>>(
          CppToType<T>::create(), kRows, pool.get());
      std::memcpy(
          vec->mutableRawValues(), data.data(), kRows * sizeof(T));
      return std::static_pointer_cast<BaseVector>(vec);
    };
    v0 = flat(c0);
    v1 = flat(c1);
    v2 = flat(c2);
    v3 = flat(c3);

    Sizes sizes{8, 4, 2, 1};
    method = std::make_unique<Method128>(
        ColumnRawPtrs{v0, v1, v2, v3}, sizes, nullptr);
  }
};

Fixture& fixture() {
  static Fixture f;
  return f;
}

BENCHMARK(packFixedScalar, n) {
  auto& f = fixture();
  UInt128 sink{};
  for (unsigned iter = 0; iter < n; ++iter) {
    for (size_t r = 0; r < Fixture::kRows; ++r) {
      UInt128 k = f.method->packRowScalar(r);
      sink.words[0] ^= k.words[0];
      sink.words[1] ^= k.words[1];
    }
  }
  folly::doNotOptimizeAway(sink);
}

BENCHMARK_RELATIVE(packFixedShuffleSsse3, n) {
  auto& f = fixture();
  UInt128 sink{};
  for (unsigned iter = 0; iter < n; ++iter) {
    for (size_t r = 0; r < Fixture::kRows; ++r) {
      UInt128 k = f.method->packRowSsse3(r);
      sink.words[0] ^= k.words[0];
      sink.words[1] ^= k.words[1];
    }
  }
  folly::doNotOptimizeAway(sink);
}

bool ssse3AvailableForBench() {
  return Method128::ssse3Available();
}

} // namespace
} // namespace facebook::velox::exec::ch

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  const bool ssse3 =
      facebook::velox::exec::ch::ssse3AvailableForBench();
  LOG(INFO) << "ch KeysFixed SSSE3 A/B benchmark: ssse3Available="
            << (ssse3 ? "true" : "false (ARM/no-SSSE3 -> ssse3 path falls back "
                                 "to scalar)");
  folly::runBenchmarks();
  return 0;
}
