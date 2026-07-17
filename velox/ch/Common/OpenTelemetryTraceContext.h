#pragma once

#include <string_view>

namespace facebook::velox::ch {

namespace OpenTelemetry {

class SpanHolder {
 public:
  explicit SpanHolder(std::string_view) {}

  template <typename T>
  void addAttribute(std::string_view, const T&) {}
};

} // namespace OpenTelemetry

} // namespace facebook::velox::ch
