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
#include <cstddef>
#include <cstdint>
#include <iterator>

#include "velox/exec/ch/Arena.h"
#include "velox/exec/ch/RowRef.h"

namespace facebook::velox::exec::ch {

// Ported from ClickHouse Interpreters/RowRefs.h RowRefList/Batch. The lazy
// chained representation of "one key, many matching right rows": a cell holds a
// single 8-byte tagged word (bit 63 set => inline single RowRef; word != 0 and
// bit 63 clear => 48-bit Batch pointer + 15-bit saturating count in bits
// 62..48). Nodes are allocated only when a key's first duplicate arrives, so
// unique keys never touch the arena. Infra: the block source is Velox
// ch::Arena (task02) instead of the ClickHouse arena; the algorithm is
// unchanged.
struct RowRefList {
  // Low 48 bits of a list word hold the node pointer; bits 62..48 hold the
  // saturating count. See task01 RowRef.h / the CH comment for why 48 bits are
  // enough (user-space mappings stay below the 47-bit boundary).
  static constexpr uint64_t PTR_MASK = (1ull << 48) - 1;
  static constexpr uint32_t COUNT_SHIFT = 48;
  // Sentinel meaning "count >= COUNT_SAT, load total_rows from the node".
  static constexpr uint32_t COUNT_SAT = 0x7FFFu;

  // A single 64-byte node. The cell word always points at the FIRST ("cell")
  // node of a key. refs[0] (the head) and the local slots are one contiguous
  // refs array so the iterator can walk them as one run.
  //
  // Cell node, unchained (2..7 rows): refs[0] first row, refs[1..size-1] rest,
  //   size == total_rows, no pointers.
  // Cell node, chained (>= 8 rows): refs[0..5] the 6 oldest rows, refs[SLOTS]
  //   (= refs[6]) a raw pointer to the NEWEST overflow node, size == 6.
  // Range node: is_range == 1, refs[0] range start, total_rows the run length.
  // Overflow node: refs[0] is the raw pointer to the next-older overflow node
  //   (0 at the end), refs[1..size] hold refs.
  //
  // Iteration order: refs[0], the cell node's local refs, then the overflow
  // nodes newest-first.
  struct Batch {
    // Local ref slots besides refs[0] (the head).
    static constexpr size_t SLOTS = 6;

    uint64_t is_range : 1 = 0;
    uint64_t size : 7 = 0;        // cell node: local rows incl. head; overflow: local refs
    uint64_t total_rows : 56 = 0; // whole chain; authoritative in the cell node only
    // One contiguous run: refs[0] is the head (cell node: first ref word;
    // overflow node: next-older Batch*); refs[1..SLOTS] are the local slots.
    // Arena::alloc skips ctors, so insert sets the occupied prefix by hand.
    uint64_t refs[SLOTS + 1]{};
  };

  // refs[0] + the SLOTS local slots: rows a cell node holds before it chains.
  static constexpr size_t MAX_LOCAL = 1 + Batch::SLOTS;

  uint64_t word = 0;

  RowRefList() = default;
  RowRefList(uint32_t block_no, uint32_t row_no)
      : word(RowRef(block_no, row_no).encode()) {}

  // View an encoded cell word as a RowRefList.
  static RowRefList fromWord(uint64_t word_) {
    RowRefList list;
    list.word = word_;
    return list;
  }

  bool isInline() const {
    return refWordIsInline(word);
  }

  const Batch* asBatch() const {
    assert(word != 0 && !isInline());
    return reinterpret_cast<const Batch*>(word & PTR_MASK);
  }

  Batch* asBatch() {
    assert(word != 0 && !isInline());
    return reinterpret_cast<Batch*>(word & PTR_MASK);
  }

  // Total rows for this key. Load-free unless the count saturated.
  uint32_t rows() const {
    if (isInline()) {
      return 1;
    }
    const uint32_t count =
        static_cast<uint32_t>((word >> COUNT_SHIFT) & COUNT_SAT);
    if (count != COUNT_SAT) {
      return count;
    }
    return static_cast<uint32_t>(asBatch()->total_rows);
  }

  // Encoded ref word of the first row (any-row semantics).
  uint64_t firstWord() const {
    return isInline() ? word : asBatch()->refs[0];
  }

  void setRange(uint64_t start_word, size_t rows_, Arena& pool) {
    assert(refWordIsInline(start_word));

    if (rows_ == 1) {
      word = start_word;
      return;
    }

    auto* b = pool.alloc<Batch>();
    b->is_range = 1;
    b->size = 0;
    b->total_rows = rows_;
    b->refs[0] = start_word;
    setListWord(b, rows_);
  }

  // Insert one more row for this key. O(1). See the Batch comment.
  void insert(uint64_t ref_word, Arena& pool) {
    assert(refWordIsInline(ref_word));

    // First row: store it inline (no allocation).
    if (word == 0) {
      word = ref_word;
      return;
    }

    // Second row: allocate the cell node and move the inline ref into its head.
    if (isInline()) {
      auto* b = pool.alloc<Batch>();
      b->is_range = 0;
      b->size = 2;
      b->total_rows = 2;
      b->refs[0] = word;
      b->refs[1] = ref_word;
      setListWord(b, 2);
      return;
    }

    Batch* b = asBatch();
    assert(!b->is_range);
    const uint64_t new_total = b->total_rows + 1;

    if (b->size == b->total_rows) { // unchained cell node
      if (b->size < MAX_LOCAL) { // room left in slots
        b->refs[b->size] = ref_word;
        b->size = b->size + 1;
      } else { // full: evict the last local ref into a new overflow node
        auto* n = pool.alloc<Batch>();
        n->is_range = 0;
        n->size = 2;
        n->total_rows = 0;
        n->refs[0] = 0; // no older node yet
        n->refs[1] = b->refs[Batch::SLOTS]; // the evicted last local ref
        n->refs[2] = ref_word;
        b->refs[Batch::SLOTS] = reinterpret_cast<uint64_t>(n);
        b->size = MAX_LOCAL - 1; // refs[0] + (SLOTS-1) local refs remain
      }
    } else { // chained cell node: append into the newest overflow node
      auto* newest = reinterpret_cast<Batch*>(b->refs[Batch::SLOTS]);
      if (newest->size < Batch::SLOTS) {
        newest->refs[newest->size + 1] = ref_word;
        newest->size = newest->size + 1;
      } else {
        auto* n = pool.alloc<Batch>();
        n->is_range = 0;
        n->size = 1;
        n->total_rows = 0;
        n->refs[0] = reinterpret_cast<uint64_t>(newest); // next-older node
        n->refs[1] = ref_word;
        b->refs[Batch::SLOTS] = reinterpret_cast<uint64_t>(n);
      }
    }

    b->total_rows = new_total;
    setListWord(b, new_total);
  }

  // Iterates encoded ref words: refs[0] first, then the cell node's local refs,
  // then the overflow nodes newest-first. Handles inline, list, and range.
  class ForwardIterator {
   public:
    explicit ForwardIterator(const RowRefList& list) {
      if (list.word == 0) {
        return; // empty list -> ok() is false
      }

      if (list.isInline()) {
        range_word = list.word;
        range_remaining = 1;
        return;
      }

      const Batch* b = list.asBatch();
      if (b->is_range) {
        range_word = b->refs[0];
        range_remaining = static_cast<uint32_t>(b->total_rows);
        return;
      }

      const bool chained = b->size != b->total_rows;
      cur = &b->refs[0];
      run_end =
          &b->refs[0] + (chained ? Batch::SLOTS : static_cast<size_t>(b->size));
      next_node = chained
          ? reinterpret_cast<const Batch*>(b->refs[Batch::SLOTS])
          : nullptr;
    }

    uint64_t operator*() const {
      return cur ? *cur : range_word;
    }

    void operator++() {
      if (cur) { // run mode - the hot path
        ++cur;
        if (cur == run_end) {
          if (next_node) {
            cur = &next_node->refs[1];
            run_end = &next_node->refs[1] + next_node->size;
            next_node = reinterpret_cast<const Batch*>(next_node->refs[0]);
          } else {
            cur = nullptr; // exhausted
          }
        }
        return;
      }

      // Range mode: consecutive rows live in one block, only row_no advances.
      if (--range_remaining) {
        ++range_word;
      }
    }

    bool ok() const {
      return cur != nullptr || range_remaining != 0;
    }

    bool operator!=(std::default_sentinel_t) const {
      return ok();
    }

   private:
    const uint64_t* cur = nullptr;
    const uint64_t* run_end = nullptr;
    const Batch* next_node = nullptr;
    uint64_t range_word = 0;
    uint32_t range_remaining = 0;
  };

  ForwardIterator begin() const {
    return ForwardIterator(*this);
  }
  std::default_sentinel_t end() const {
    return {};
  }

 private:
  // Repoint word at b with the saturating row count in bits 62..48.
  void setListWord(Batch* b, uint64_t total_rows_) {
    const uint64_t ptr = reinterpret_cast<uint64_t>(b);
    // A Batch pointer that does not fit in 48 bits would make the pointer+count
    // packing ambiguous. task02 verified ch::Arena hands out addresses below
    // 2^48, so this is a defensive check (see CH throwRowRefPointerTooLarge).
    assert((ptr & ~PTR_MASK) == 0);
    const uint64_t count = total_rows_ < COUNT_SAT ? total_rows_ : COUNT_SAT;
    word = ptr | (count << COUNT_SHIFT);
  }
};

static_assert(sizeof(RowRefList) == 8);
static_assert(sizeof(RowRefList::Batch) == 64);

} // namespace facebook::velox::exec::ch
