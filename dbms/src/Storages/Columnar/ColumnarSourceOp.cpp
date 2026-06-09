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

#include <Storages/Columnar/ColumnarSourceOp.h>

#include <Common/Stopwatch.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>

namespace DB
{
RNProxySourceOp::RNProxySourceOp(const Options & options)
    : SourceOp(options.exec_context, options.task->getLog()->identifier())
    , context(options.task->getContext())
    , log(options.task->getLog())
    , task(options.task)
{
    setHeader(AddExtraTableIDColumnTransformAction::buildHeader(
        options.task->getColumnsToRead(),
        options.task->getExtraTableIDIndex()));
}

void RNProxySourceOp::operateSuffixImpl()
{
    UNUSED(context);
    const double total_cost_sec = total_cost_watch.elapsedSeconds();
    const UInt64 rows_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_rows) / total_cost_sec) : 0;
    const UInt64 bytes_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_bytes) / total_cost_sec) : 0;
    LOG_INFO(
        log,
        "Finished reading proxy snapshots, task_pool_worker_total_cost={:.3f}s claimed_streams={} rows={} "
        "rows_per_sec={} "
        "bytes={} bytes_per_sec={} read_cost={:.3f}s",
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void RNProxySourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
    LOG_INFO(log, "Begin reading proxy snapshots");
}

void RNProxySourceOp::releaseCurrentReader()
{
    current_input_stream.reset();
    current_reader_slot.reset();
    current_reader_idx.reset();
}

void RNProxySourceOp::attachInputStreamForCurrentReader()
{
    RUNTIME_CHECK(source_state == RNProxySourceState::Reading);
    RUNTIME_CHECK(current_reader_idx.has_value());
    RUNTIME_CHECK(!current_input_stream);

    const auto reader_index = current_reader_idx.value();
    if (auto ready_reader = task->tryTakeReadyReader(reader_index); ready_reader.has_value())
    {
        current_input_stream
            = task->createInputStreamWithReader(reader_index, std::move(ready_reader.value()));
    }
    else
    {
        current_input_stream = task->createInputStreamWithReader(
            reader_index,
            task->materializeReaderInIOThread(reader_index));
    }
    ++total_streams;
}

OperatorStatus RNProxySourceOp::scheduleWaitReader()
{
    RUNTIME_CHECK(source_state == RNProxySourceState::WaitReader);
    RUNTIME_CHECK(current_reader_idx.has_value());
    RUNTIME_CHECK(current_reader_slot != nullptr);

    const auto reader_index = current_reader_idx.value();
    const auto slot_state = task->getReaderMaterializeState(reader_index);
    switch (slot_state)
    {
    case RNProxyReaderMaterializeState::Ready:
    case RNProxyReaderMaterializeState::NotStarted:
        source_state = RNProxySourceState::Reading;
        return OperatorStatus::IO_IN;
    case RNProxyReaderMaterializeState::Creating:
        setNotifyFuture(current_reader_slot.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case RNProxyReaderMaterializeState::Failed:
        task->rethrowReaderSlotException(reader_index);
    case RNProxyReaderMaterializeState::Consumed:
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "proxy reader {} already consumed before pipeline read",
            reader_index);
    }
}

OperatorStatus RNProxySourceOp::scheduleAcquireReader()
{
    RUNTIME_CHECK(source_state == RNProxySourceState::AcquireReader);
    RUNTIME_CHECK(!current_reader_idx.has_value());

    auto next_reader_idx = task->tryAcquireReaderIndex();
    if (!next_reader_idx.has_value())
    {
        source_state = RNProxySourceState::Done;
        return OperatorStatus::HAS_OUTPUT;
    }

    current_reader_idx = next_reader_idx;
    current_reader_slot = task->getReaderSlot(current_reader_idx.value());
    // Prefetch the next reader asynchronously; the current one is materialized on the IO thread.
    task->prefetchReader(current_reader_idx.value() + 1);

    const auto slot_state = task->getReaderMaterializeState(current_reader_idx.value());
    switch (slot_state)
    {
    case RNProxyReaderMaterializeState::Ready:
    case RNProxyReaderMaterializeState::NotStarted:
        source_state = RNProxySourceState::Reading;
        return OperatorStatus::IO_IN;
    case RNProxyReaderMaterializeState::Creating:
        source_state = RNProxySourceState::WaitReader;
        return scheduleWaitReader();
    case RNProxyReaderMaterializeState::Failed:
        task->rethrowReaderSlotException(current_reader_idx.value());
    case RNProxyReaderMaterializeState::Consumed:
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "proxy reader {} already consumed before pipeline read",
            current_reader_idx.value());
    }
}

OperatorStatus RNProxySourceOp::scheduleNextAction()
{
    switch (source_state)
    {
    case RNProxySourceState::Done:
    case RNProxySourceState::ReadyBlock:
        return OperatorStatus::HAS_OUTPUT;
    case RNProxySourceState::Reading:
        return OperatorStatus::IO_IN;
    case RNProxySourceState::WaitReader:
        return scheduleWaitReader();
    case RNProxySourceState::AcquireReader:
        return scheduleAcquireReader();
    }
}

OperatorStatus RNProxySourceOp::readImpl(Block & block)
{
    switch (source_state)
    {
    case RNProxySourceState::Done:
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    case RNProxySourceState::ReadyBlock:
    {
        RUNTIME_CHECK(t_block.has_value());
        std::swap(block, t_block.value());
        t_block.reset();
        source_state = RNProxySourceState::AcquireReader;
        return OperatorStatus::HAS_OUTPUT;
    }
    case RNProxySourceState::Reading:
    case RNProxySourceState::WaitReader:
    case RNProxySourceState::AcquireReader:
    {
        // CPU path only: schedule IO / notify without proxy FFI.
        const auto status = scheduleNextAction();
        if (source_state == RNProxySourceState::Done)
            block = {};
        return status;
    }
    }
}

OperatorStatus RNProxySourceOp::awaitImpl()
{
    // Non-blocking state check only. RNProxy source does not use OperatorStatus::WAITING.
    if (source_state == RNProxySourceState::Done || source_state == RNProxySourceState::ReadyBlock)
        return OperatorStatus::HAS_OUTPUT;
    return scheduleNextAction();
}

OperatorStatus RNProxySourceOp::executeIOImpl()
{
    RUNTIME_CHECK(source_state == RNProxySourceState::Reading);
    RUNTIME_CHECK(current_reader_idx.has_value());

    if (!current_input_stream)
        attachInputStreamForCurrentReader();

    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block = current_input_stream->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();

    // At most one block per IO task. Yield back to CPU with NEED_INPUT when the reader is exhausted.
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        total_bytes += block.bytes();
        t_block.emplace(std::move(block));
        source_state = RNProxySourceState::ReadyBlock;
        releaseCurrentReader();
        return OperatorStatus::HAS_OUTPUT;
    }

    releaseCurrentReader();
    source_state = RNProxySourceState::AcquireReader;
    return OperatorStatus::NEED_INPUT;
}
} // namespace DB
#endif
