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

#include "velox/type/Type.h"

#include <array>
#include <map>
#include <mutex>

namespace facebook::velox::exec::ch2 {

// ============================================================================
// FixedStringType(N) — O3 的 infra 承载逻辑类型。
//
// 背景(铁律 + O3):CH 有物理类型 `ColumnFixedString`(定长 N 字节字符串,
// getN() + getChars() 连续 buffer,第 row 行 key = chars[row*n .. row*n+n])。
// Velox **没有** FixedString 物理类型 —— Velox 的字符串都是变长
// VARCHAR/VARBINARY(FlatVector<StringView>,每行自带 ptr+size)。
//
// 按铁律「没有的物理类型 → 定义一个 Velox 逻辑类型来承载数据」:
//   - 物理承载:VARBINARY(继承 VarbinaryType = ScalarType<VARBINARY>),
//     数据装在 FlatVector<StringView> 里,约定每行 StringView 正好 N 字节。
//   - 逻辑标记:N。取值算法(HashMethodFixedString::getKeyHolder)从这个类型
//     拿 N,再 slice N 字节。
//   - **这是纯 infra**(让 Velox 能装「定长 N 字节」数据),不含算法。
//
// N 参数化机制(本 task 的 infra 难点)——核实结论:
//   Velox 的带参 custom type 走 `TypeParameter`(type/Type.h TypeParameterKind
//   ::kLongLiteral)承载数值参数。范例 `DecimalType`(type/Type.h:840)就把
//   precision/scale 存成 `std::array<TypeParameter,2> parameters_`,
//   `equivalent()` 比较两个参数、`parameters()` 暴露给类型系统。
//   HyperLogLogType(functions/prestosql/types/HyperLogLogType.h)则是**无参
//   单例**(继承 VarbinaryType,pointer 比较)—— 不适用带 N 参数的场景。
//   所以 FixedStringType 采「DecimalType 式带参 + VarbinaryType 式物理承载」:
//     * 继承 VarbinaryType(物理 = VARBINARY),
//     * 把 N 存成一个 `TypeParameter{kLongLiteral, N}`(与 DecimalType 一致的
//       参数承载机制),`parameters()` 暴露、`equivalent()` 比较 N,
//     * 工厂 `FIXED_STRING(n)` 按 N 缓存一个实例(每个 N 一个类型实例,类型
//       内部带 N 参数)。
// ============================================================================
class FixedStringType : public VarbinaryType {
 public:
  // N=0 无意义;不允许。
  explicit FixedStringType(uint32_t n)
      : parameters_{TypeParameter(static_cast<int64_t>(n))} {
    VELOX_CHECK_GT(n, 0, "FixedStringType requires N > 0");
  }

  // 逻辑标记:定长字节数 N(从 TypeParameter 取,机制同 DecimalType::precision)。
  uint32_t fixedLength() const {
    return static_cast<uint32_t>(parameters_[0].longLiteral.value());
  }

  bool equivalent(const Type& other) const override {
    if (!Type::hasSameTypeId(other)) {
      return false;
    }
    const auto* otherFixed = dynamic_cast<const FixedStringType*>(&other);
    if (otherFixed == nullptr) {
      return false;
    }
    return otherFixed->fixedLength() == fixedLength();
  }

  const char* name() const override {
    return "FIXEDSTRING";
  }

  std::string toString() const override {
    return fmt::format("FIXEDSTRING({})", fixedLength());
  }

  std::span<const TypeParameter> parameters() const override {
    return parameters_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    obj["fixedLength"] = fixedLength();
    return obj;
  }

  bool isOrderable() const override {
    return false;
  }

 private:
  const std::array<TypeParameter, 1> parameters_;
};

// 工厂:按 N 缓存一个 FixedStringType 实例(每个 N 一个类型实例,类型带 N 参
// 数)。返回 shared_ptr,契合 Velox TypePtr。
inline std::shared_ptr<const FixedStringType> FIXED_STRING(uint32_t n) {
  static std::map<uint32_t, std::shared_ptr<const FixedStringType>> cache;
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);
  auto it = cache.find(n);
  if (it != cache.end()) {
    return it->second;
  }
  auto type = std::make_shared<const FixedStringType>(n);
  cache.emplace(n, type);
  return type;
}

inline bool isFixedStringType(const TypePtr& type) {
  return dynamic_cast<const FixedStringType*>(type.get()) != nullptr;
}

} // namespace facebook::velox::exec::ch2
