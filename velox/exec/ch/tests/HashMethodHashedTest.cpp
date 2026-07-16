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

#include "velox/exec/ch/Common/ColumnsHashing/FixedKey.h" // ch::UInt128
#include "velox/exec/ch/Common/HashTable/HashMap.h" // ch::HashMapAll_hashed
#include "velox/exec/ch/Common/ColumnsHashing/HashMethod.h"
#include "velox/exec/ch/Common/SipHash.h"
#include "velox/exec/ch/Interpreters/HashJoin/ChHashMethodDispatch.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/ch/Common/Arena.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

namespace facebook::velox::exec::ch {
namespace {

using ch::UInt128;

// digest map reuses port HashMapAll_hashed (UInt128 + UInt128TrivialHash taking
// words[0]) -- carrier layer, CH-consistent (CH also HashMap<UInt128,
// UInt128TrivialHash>).
using MapHashed = ch::HashMapAll_hashed;

using MethodHashed = HashMethodHashed<
    MapHashed::value_type,
    MapHashed::mapped_type,
    /*use_cache=*/false>;

class HashMethodHashedTest : public testing::Test,
                             public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::Options options;
      options.trackDefaultUsage = true;
      memory::MemoryManager::initialize(options);
    }
  }

  void SetUp() override {
    mapPool_ = memory::memoryManager()->addLeafPool("ch-hmh-map");
    arenaPool_ = memory::memoryManager()->addLeafPool("ch-hmh-arena");
  }

  std::shared_ptr<memory::MemoryPool> mapPool_;
  std::shared_ptr<memory::MemoryPool> arenaPool_;

  // Independent reference digest: explicitly feed bytes into ch::SipHash the
  // exact way CH IColumn::updateHashWithValue does (numeric: sizeof value
  // bytes; string: size+1 as size_t, then bytes, then UInt8(0)), then get128().
  // This is a separate code path from hash128; byte-for-byte equality proves
  // hash128's byte feeding matches CH updateHashWithValue.
  static UInt128 refI64(const std::vector<const int64_t*>& cols, size_t row) {
    SipHash h;
    for (const auto* c : cols) {
      const int64_t v = c[row];
      // CH ColumnVector<Int64>::updateHashWithValue = hash.update(data[n]).
      h.update(reinterpret_cast<const char*>(&v), sizeof(v));
    }
    return h.get128();
  }

  static UInt128 refStr(const std::vector<std::string>& vals) {
    SipHash h;
    for (const auto& s : vals) {
      // CH ColumnString::updateHashWithValue.
      const size_t size_used_in_hash = s.size() + 1;
      h.update(
          reinterpret_cast<const char*>(&size_used_in_hash),
          sizeof(size_used_in_hash));
      h.update(s.data(), s.size());
      h.update(UInt8(0));
    }
    return h.get128();
  }
};

// hash128 cross-check (numeric multi-column): ch HashMethodHashed::getKeyHolder
// digest == independent reference (explicit CH updateHashWithValue byte feed
// into ch::SipHash). Byte-for-byte equality proves byte feeding matches CH.
TEST_F(HashMethodHashedTest, digestNumericMatchesChByteFeed) {
  const std::vector<int64_t> c0{10, 20, 10, 30, 20};
  const std::vector<int64_t> c1{100, 200, 100, 300, 999};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);

  MethodHashed method({v0, v1}, {}, nullptr);
  ch::Arena arena(arenaPool_.get());

  std::vector<const int64_t*> cols{v0->rawValues(), v1->rawValues()};
  for (size_t r = 0; r < rows; ++r) {
    UInt128 got = method.getKeyHolder(r, arena);
    UInt128 want = refI64(cols, r);
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt128)))
        << "numeric digest mismatch row " << r;
  }
  // row0 == row2 -> equal digest.
  UInt128 d0 = method.getKeyHolder(0, arena);
  UInt128 d2 = method.getKeyHolder(2, arena);
  EXPECT_EQ(0, std::memcmp(&d0, &d2, sizeof(UInt128)));
}

// hash128 cross-check (string multi-column): proves string size+1/bytes/0
// three-part feed matches CH.
TEST_F(HashMethodHashedTest, digestStringMatchesChByteFeed) {
  const std::vector<std::string> a{"apple", "banana", "apple", ""};
  const std::vector<std::string> b{"x", "yy", "x", "zzz"};
  const size_t rows = a.size();

  auto va = makeFlatVector<std::string>(a);
  auto vb = makeFlatVector<std::string>(b);

  MethodHashed method({va, vb}, {}, nullptr);
  ch::Arena arena(arenaPool_.get());

  for (size_t r = 0; r < rows; ++r) {
    UInt128 got = method.getKeyHolder(r, arena);
    UInt128 want = refStr({a[r], b[r]});
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt128)))
        << "string digest mismatch row " << r;
  }
}

// hash128 cross-check (numeric + string mixed columns).
TEST_F(HashMethodHashedTest, digestMixedMatchesChByteFeed) {
  const std::vector<int64_t> n{7, 8, 7};
  const std::vector<std::string> s{"hi", "yo", "hi"};
  const size_t rows = n.size();

  auto vn = makeFlatVector<int64_t>(n);
  auto vs = makeFlatVector<std::string>(s);

  MethodHashed method({vn, vs}, {}, nullptr);
  ch::Arena arena(arenaPool_.get());

  for (size_t r = 0; r < rows; ++r) {
    UInt128 got = method.getKeyHolder(r, arena);
    // Independent reference: feed int64 (sizeof bytes) then string
    // (size+1/bytes/0).
    SipHash h;
    const int64_t nv = n[r];
    h.update(reinterpret_cast<const char*>(&nv), sizeof(nv));
    const size_t sz = s[r].size() + 1;
    h.update(reinterpret_cast<const char*>(&sz), sizeof(sz));
    h.update(s[r].data(), s[r].size());
    h.update(UInt8(0));
    UInt128 want = h.get128();
    EXPECT_EQ(0, std::memcmp(&got, &want, sizeof(UInt128)))
        << "mixed digest mismatch row " << r;
  }
}

// build/find via digest map: multi-column key insert->find correct; digest
// equality is the hit criterion (HashMapAll_hashed uses UInt128TrivialHash
// taking words[0]).
TEST_F(HashMethodHashedTest, emplaceFindMultiColumn) {
  const std::vector<int64_t> c0{10, 20, 10, 30, 20};
  const std::vector<int64_t> c1{100, 200, 100, 300, 999};
  const size_t rows = c0.size();

  auto v0 = makeFlatVector<int64_t>(c0);
  auto v1 = makeFlatVector<int64_t>(c1);

  MethodHashed method({v0, v1}, {}, nullptr);
  MapHashed map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // distinct = {(10,100),(20,200),(30,300),(20,999)} = 4 (row0==row2).
  size_t inserted = ChHashMethodDispatch::build(method, map, rows, arena);
  EXPECT_EQ(inserted, 4);
  EXPECT_EQ(map.size(), 4);
  for (size_t r = 0; r < rows; ++r) {
    EXPECT_TRUE(method.findKey(map, r, arena).isFound()) << "row " << r;
  }

  // probe: (10,999) not in build set; (20,200) is.
  auto p0 = makeFlatVector<int64_t>(std::vector<int64_t>{10, 20});
  auto p1 = makeFlatVector<int64_t>(std::vector<int64_t>{999, 200});
  MethodHashed probe({p0, p1}, {}, nullptr);
  EXPECT_FALSE(probe.findKey(map, 0, arena).isFound());
  EXPECT_TRUE(probe.findKey(map, 1, arena).isFound());
}

// String key through digest map build/find.
TEST_F(HashMethodHashedTest, emplaceFindStringKey) {
  const std::vector<std::string> a{"apple", "banana", "apple", "cherry"};
  const size_t rows = a.size();
  auto va = makeFlatVector<std::string>(a);

  MethodHashed method({va}, {}, nullptr);
  MapHashed map(mapPool_.get());
  ch::Arena arena(arenaPool_.get());

  // distinct = {apple, banana, cherry} = 3.
  size_t inserted = ChHashMethodDispatch::build(method, map, rows, arena);
  EXPECT_EQ(inserted, 3);
  for (size_t r = 0; r < rows; ++r) {
    EXPECT_TRUE(method.findKey(map, r, arena).isFound()) << "row " << r;
  }

  auto miss = makeFlatVector<std::string>(std::vector<std::string>{"durian"});
  MethodHashed probe({miss}, {}, nullptr);
  EXPECT_FALSE(probe.findKey(map, 0, arena).isFound());
}

} // namespace
} // namespace facebook::velox::exec::ch
