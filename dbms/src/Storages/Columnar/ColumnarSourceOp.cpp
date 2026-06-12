// Copyright 2023 PingCAP, Inc.
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
#include <Common/Stopwatch.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Storages/Columnar/ColumnarSourceOp.h>
#include <common/logger_useful.h>

namespace DB
{

void RNColumnarSourceOp::operateSuffixImpl()
{
    UNUSED(context);
    const auto keyspace_id = exec_context.getKeyspaceID();
    const double total_cost_sec = total_cost_watch.elapsedSeconds();
    const UInt64 rows_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_rows) / total_cost_sec) : 0;
    const UInt64 bytes_per_sec
        = total_cost_sec > 0 ? static_cast<UInt64>(static_cast<double>(total_bytes) / total_cost_sec) : 0;
    LOG_INFO(
        log,
        "Finished reading columnar snapshots, keyspace_id={} task_pool_worker_total_cost={:.3f}s claimed_streams={} "
        "rows={} "
        "rows_per_sec={} "
        "bytes={} bytes_per_sec={} read_cost={:.3f}s",
        keyspace_id,
        total_cost_sec,
        total_streams,
        total_rows,
        rows_per_sec,
        total_bytes,
        bytes_per_sec,
        duration_read_sec);
}

void RNColumnarSourceOp::operatePrefixImpl()
{
    total_cost_watch.restart();
    LOG_INFO(log, "Begin reading columnar snapshots, keyspace_id={}", exec_context.getKeyspaceID());
}

OperatorStatus RNColumnarSourceOp::readImpl(Block & block)
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

    return awaitImpl();
}

OperatorStatus RNColumnarSourceOp::awaitImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    // If we are waiting for a prefetch thread to finish materializing a reader,
    // check the state without blocking.
    if (current_reader_work)
    {
        std::optional<ColumnarReaderPtr> ready_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                // Prefetch finished — consume the reader under lock, then proceed to IO.
                ready_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                break;
            case RNColumnarReaderMaterializeState::Failed:
                std::rethrow_exception(current_reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    current_reader_work->plan.region_id);
            case RNColumnarReaderMaterializeState::Creating:
                // Still being materialized — register for wakeup.
                setNotifyFuture(&current_reader_work->notify_future);
                return OperatorStatus::WAIT_FOR_NOTIFY;
            case RNColumnarReaderMaterializeState::NotStarted:
                // Should not happen here: executeIOImpl transitions NotStarted → Creating
                // before returning control. Fall through to IO_IN as a safety net.
                return OperatorStatus::IO_IN;
            }
        }

        if (ready_reader.has_value())
        {
            current_input_stream = RNColumnarInputStream::createWithReader(
                {
                    .context = context,
                    .log = log,
                    .task = task,
                    .reader_work = current_reader_work,
                    .columns_to_read = task->getColumnsToRead(),
                    .extra_table_id_index = task->getExtraTableIDIndex(),
                    .table_id = task->getLogicalTableID(),
                    .executor_id = task->getExecutorID(),
                },
                std::move(ready_reader.value()));
            current_reader_work.reset();
            ++total_streams;
            return OperatorStatus::IO_IN;
        }
    }

    // No current work — need to acquire one in executeIOImpl.
    return OperatorStatus::IO_IN;
}

OperatorStatus RNColumnarSourceOp::executeIOImpl()
{
    if (unlikely(done || t_block.has_value()))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    if (!current_input_stream)
    {
        // Acquire a reader work if we don't already have one.
        if (!current_reader_work)
        {
            auto next_work = task->tryAcquireReaderWork();
            if (!next_work.has_value())
            {
                done = true;
                return OperatorStatus::HAS_OUTPUT;
            }
            current_reader_work = std::move(next_work.value());
        }

        // Single lock acquisition to atomically check state and decide next action,
        // avoiding a TOCTOU race with the prefetch thread.
        bool should_materialize = false;
        std::optional<ColumnarReaderPtr> ready_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                ready_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                break;
            case RNColumnarReaderMaterializeState::Failed:
                std::rethrow_exception(current_reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    current_reader_work->plan.region_id);
            case RNColumnarReaderMaterializeState::NotStarted:
                current_reader_work->state = RNColumnarReaderMaterializeState::Creating;
                should_materialize = true;
                break;
            case RNColumnarReaderMaterializeState::Creating:
                // Prefetch is still working on this reader. Yield so awaitImpl can
                // register for WAIT_FOR_NOTIFY instead of blocking the IO thread.
                return awaitImpl();
            }
        }

        if (ready_reader.has_value())
        {
            // Prefetch completed before we even needed to wait.
            current_input_stream = RNColumnarInputStream::createWithReader(
                {
                    .context = context,
                    .log = log,
                    .task = task,
                    .reader_work = current_reader_work,
                    .columns_to_read = task->getColumnsToRead(),
                    .extra_table_id_index = task->getExtraTableIDIndex(),
                    .table_id = task->getLogicalTableID(),
                    .executor_id = task->getExecutorID(),
                },
                std::move(ready_reader.value()));
            current_reader_work.reset();
            ++total_streams;
        }
        else
        {
            RUNTIME_CHECK(should_materialize);
            // Materialize the reader inline in the IO thread (involves FFI / backoff).
            auto reader = task->createColumnarReaderWithBackoff(current_reader_work);
            {
                std::lock_guard lock(current_reader_work->mutex);
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
            }
            // Wake any waiters (stream path via cv, pipeline path via notify_future).
            current_reader_work->cv.notify_all();
            current_reader_work->notify_future.notifyAll();

            current_input_stream = RNColumnarInputStream::createWithReader(
                {
                    .context = context,
                    .log = log,
                    .task = task,
                    .reader_work = current_reader_work,
                    .columns_to_read = task->getColumnsToRead(),
                    .extra_table_id_index = task->getExtraTableIDIndex(),
                    .table_id = task->getLogicalTableID(),
                    .executor_id = task->getExecutorID(),
                },
                std::move(reader));
            current_reader_work.reset();
            ++total_streams;
        }
    }

    // Read one block from the current input stream.
    FilterPtr filter_ignored = nullptr;
    Stopwatch w{CLOCK_MONOTONIC_COARSE};
    Block block = current_input_stream->read(filter_ignored, false);
    duration_read_sec += w.elapsedSeconds();
    if likely (block && block.rows() > 0)
    {
        total_rows += block.rows();
        total_bytes += block.bytes();
        t_block.emplace(std::move(block));
        return OperatorStatus::HAS_OUTPUT;
    }
    else
    {
        current_input_stream.reset();
        return awaitImpl();
    }
}

} // namespace DB
#endif
