#pragma once

#include "velox/common/testutil/TestValue.h"

/// FileCache fault-injection points.
///
/// In ClickHouse these are fiu-based failpoints (`fiu_do_on(name, { throw ...;
/// })`). Here `FAIL_POINT_TRIGGER(name)` maps to Velox's TestValue injection
/// facility: a unit test registers a callback (typically one that throws) at the
/// named point via `SCOPED_TESTVALUE_SET` / `TestValue::set` after
/// `TestValue::enable()`, and production code triggers it here.
///
/// This does not change production behavior. `TestValue::adjust` is compiled out
/// entirely in release builds (NDEBUG) and, in debug builds, returns immediately
/// unless a test has both enabled TestValue and registered a callback for this
/// exact point. It is the same seam 50+ production Velox translation units already
/// use. The full production failpoint surface (system-table registration,
/// pause/resume, config wiring) remains Task 017.
#define FAIL_POINT_TRIGGER(name)                          \
  ::facebook::velox::common::testutil::TestValue::adjust( \
      "facebook::velox::ch::filecache::failpoint::" #name, nullptr)
