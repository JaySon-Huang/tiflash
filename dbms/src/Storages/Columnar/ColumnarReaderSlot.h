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

#pragma once

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR

#include <Storages/KVStore/FFI/ProxyFFI.h>

#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>

namespace DB
{
/// Lifecycle of a single ColumnarReader materialized from proxy FFI.
/// Pipeline source operators observe these states without blocking the IO thread pool;
/// Phase 3 will replace `cv` blocking wait with PipeConditionVariable / NotifyFuture.
enum class RNProxyReaderMaterializeState
{
    NotStarted,
    Creating,
    Ready,
    Failed,
    Consumed,
};

/// Per-reader slot shared between RNProxyReadTask and RNProxySourceOp.
///
/// RNProxyReadTask owns creation/prefetch; RNProxySourceOp only reads state via
/// RNProxyReadTask accessors. Keep slot fields narrow so future notify-based waiting
/// can be added here without touching read-planning code in StorageDisaggregatedColumnar.
struct RNProxyReaderSlot
{
    ~RNProxyReaderSlot();

    std::mutex mutex;
    // TODO(Phase 3): add PipeConditionVariable for WAIT_FOR_NOTIFY based wake-up.
    std::condition_variable cv;
    RNProxyReaderMaterializeState state = RNProxyReaderMaterializeState::NotStarted;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};
} // namespace DB
#endif
