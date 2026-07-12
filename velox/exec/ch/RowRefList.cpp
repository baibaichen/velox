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

// RowRefList is fully inline in the header (mirroring ClickHouse RowRefs.h,
// which is header-only). This translation unit exists so the class is part of
// the velox_exec_ch library's source list and so the header is verified to
// compile stand-alone.
#include "velox/exec/ch/RowRefList.h"

namespace facebook::velox::exec::ch {

static_assert(sizeof(RowRefList) == 8);
static_assert(sizeof(RowRefList::Batch) == 64);

} // namespace facebook::velox::exec::ch
