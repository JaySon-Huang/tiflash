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

OperatorStatus RNProxySourceOp::awaitReaderSlotStatus()
{
    RUNTIME_CHECK(current_reader_idx.has_value());
    const auto reader_index = current_reader_idx.value();
    const auto state = task->getReaderMaterializeState(reader_index);
    switch (state)
    {
    case RNProxyReaderMaterializeState::Ready:
    case RNProxyReaderMaterializeState::NotStarted:
        return OperatorStatus::IO_IN;
    case RNProxyReaderMaterializeState::Creating:
        setNotifyFuture(task->getReaderSlot(reader_index).get());
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

OperatorStatus RNProxySourceOp::readImpl(Block & block)
{
    if (unlikely(done))
    {
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    }

    if (t_block.has_value())
    {
        std::swap(block, t_block.value());
        t_block.reset();
        return OperatorStatus::HAS_OUTPUT;
    }

    const auto status = awaitImpl();
    if (status == OperatorStatus::HAS_OUTPUT && done)
        block = {};
    return status;
}

OperatorStatus RNProxySourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
        return OperatorStatus::HAS_OUTPUT;

    if (!current_reader_idx.has_value())
    {
        auto next_reader_idx = task->tryAcquireReaderIndex();
        if (!next_reader_idx.has_value())
        {
            done = true;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_reader_idx = next_reader_idx;
        // Prefetch the next reader asynchronously; current reader is materialized on IO thread or taken from slot.
        task->prefetchReader(current_reader_idx.value() + 1);
    }

    // Guard against missed wake-up: if prefetch finished before task registration, go to IO path directly.
    const auto status = awaitReaderSlotStatus();
    if (status == OperatorStatus::WAIT_FOR_NOTIFY
        && task->getReaderMaterializeState(current_reader_idx.value()) != RNProxyReaderMaterializeState::Creating)
    {
        return OperatorStatus::IO_IN;
    }
    return status;
}

OperatorStatus RNProxySourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
        return OperatorStatus::HAS_OUTPUT;

    RUNTIME_CHECK(current_reader_idx.has_value());
    const auto reader_index = current_reader_idx.value();

    if (!current_input_stream)
    {
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

    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block = current_input_stream->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        total_bytes += block.bytes();
        t_block.emplace(std::move(block));
        current_input_stream.reset();
        current_reader_idx.reset();
        return OperatorStatus::HAS_OUTPUT;
    }

    current_input_stream.reset();
    current_reader_idx.reset();
    return awaitImpl();
}
} // namespace DB
#endif
