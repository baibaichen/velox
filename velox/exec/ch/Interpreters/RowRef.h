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

#include <cassert>
#include <cstdint>

namespace facebook::velox::exec::ch {

inline constexpr uint32_t kDriverNoBits = 6;
inline constexpr uint32_t kBatchNoBits = 25;
static_assert(kDriverNoBits + kBatchNoBits == 31);

// Inclusive maxima of the two sub-fields packed into the old 31-bit block_no.
// Values above these bleed across field boundaries (batch_no into driver_no) or
// into block_no bit 31 (the INLINE flag), so they are rejected below.
inline constexpr uint32_t kMaxDriverNo = (1u << kDriverNoBits) - 1;
inline constexpr uint32_t kMaxBatchNo = (1u << kBatchNoBits) - 1;

// Pack a (driver_no, batch_no) pair into the 31-bit block_no field. Like the
// defensive out-of-range guard of ClickHouse RowRef(block_no_, row_no_), an
// out-of-range coordinate is treated as a programming error (upstream limits
// already cap driver/batch counts) rather than silently corrupting the INLINE
// bit or an adjacent sub-field. Unlike CH (which throws unconditionally), this
// guards via assert(): active in Debug builds, compiled out under NDEBUG.
inline constexpr uint32_t packBlockNo(uint32_t driverNo, uint32_t batchNo) {
  assert(driverNo <= kMaxDriverNo);
  assert(batchNo <= kMaxBatchNo);
  return (driverNo << kBatchNoBits) | batchNo;
}

inline constexpr uint32_t unpackDriverNo(uint32_t blockNo) {
  return blockNo >> kBatchNoBits;
}

inline constexpr uint32_t unpackBatchNo(uint32_t blockNo) {
  return blockNo & kMaxBatchNo;
}

struct RowRef {
  static constexpr uint32_t INLINE_FLAG = 0x80000000u;
  static constexpr uint32_t BLOCK_NO_MASK = 0x7fffffffu;
  static constexpr uint64_t ENCODED_INLINE_FLAG = 1ull << 63;

  uint32_t row_no{0};
  uint32_t block_no{0};

  constexpr RowRef() = default;

  constexpr RowRef(uint32_t blockNo, uint32_t rowNo)
      : row_no(rowNo), block_no((assert(blockNo <= BLOCK_NO_MASK), blockNo | INLINE_FLAG)) {}

  constexpr uint32_t blockNo() const {
    return block_no & BLOCK_NO_MASK;
  }

  constexpr uint32_t rowNo() const {
    return row_no;
  }

  constexpr uint64_t encode() const {
    return (static_cast<uint64_t>(block_no) << 32) | row_no;
  }
};

static_assert(sizeof(RowRef) == 8);

inline constexpr bool refWordIsInline(uint64_t word) {
  return (word & RowRef::ENCODED_INLINE_FLAG) != 0;
}

inline constexpr uint32_t refWordBlockNo(uint64_t word) {
  return static_cast<uint32_t>(word >> 32) & RowRef::BLOCK_NO_MASK;
}

inline constexpr uint32_t refWordRowNo(uint64_t word) {
  return static_cast<uint32_t>(word);
}

} // namespace facebook::velox::exec::ch
