#pragma once

#include <memory>

namespace facebook::velox::ch {

class QueryStatus {
 public:
  void throwIfKilled() const {}
};

using QueryStatusPtr = std::shared_ptr<QueryStatus>;

} // namespace facebook::velox::ch
