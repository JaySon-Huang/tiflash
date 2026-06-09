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

#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>

#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>

namespace DB
{
/// Lifecycle of a single ColumnarReader materialized from proxy FFI.
/// Pipeline source operators observe these states without blocking the IO thread pool.
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
/// Pipeline path: RNProxySourceOp waits on `PipeConditionVariable` via NotifyFuture while
/// prefetch materializes the reader. Stream path: RNProxyInputStream still blocks on `cv`.
struct RNProxyReaderSlot : public NotifyFuture
{
    ~RNProxyReaderSlot();

    void registerTask(TaskPtr && task) override;

    /// Wake pipeline tasks registered on pipe_cv and stream-model waiters on cv.
    void notifyWaiters();

    std::mutex mutex;
    // Stream-model blocking wait in RNProxyReadTask::getOrCreateReader().
    std::condition_variable cv;
    // Pipeline-model WAIT_FOR_NOTIFY wake-up for RNProxySourceOp.
    PipeConditionVariable pipe_cv;
    RNProxyReaderMaterializeState state = RNProxyReaderMaterializeState::NotStarted;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};
} // namespace DB
#endif
