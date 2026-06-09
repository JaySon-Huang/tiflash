// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR

#include <Storages/Columnar/ColumnarReaderSlot.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>

namespace DB
{
RNProxyReaderSlot::~RNProxyReaderSlot()
{
    if (reader.has_value() && reader->inner.ptr != nullptr)
        RustGcHelper::instance().gcRustPtr(reader->inner.ptr, reader->inner.type);
}

void RNProxyReaderSlot::registerTask(TaskPtr && task)
{
    task->setNotifyType(NotifyType::WAIT_ON_TABLE_SCAN_READ);
    pipe_cv.registerTask(std::move(task));
}

void RNProxyReaderSlot::notifyWaiters()
{
    cv.notify_all();
    pipe_cv.notifyAll();
}
} // namespace DB
#endif
