#pragma once

namespace facebook::velox::ch {

struct FilesystemCacheLogElement {
  enum class CacheType {
    READ_FROM_CACHE,
    READ_FROM_FS_BYPASSING_CACHE,
    READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
  };

  CacheType cache_type{CacheType::READ_FROM_FS_BYPASSING_CACHE};
};

class FilesystemCacheLog {
 public:
  template <typename T>
  void add(T&&) {}
};

} // namespace facebook::velox::ch
