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
    switch (state)
    {
    case ColumnarSourceState::DONE:
        block = {};
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarSourceState::READY_BLOCK:
        assert(t_block.has_value());
        std::swap(block, t_block.value());
        t_block.reset();
        state = ColumnarSourceState::READING;
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarSourceState::NEED_READER:
    case ColumnarSourceState::WAIT_READER:
    case ColumnarSourceState::READING:
        break; // hand off to awaitImpl
    }

    return awaitImpl();
}

void RNColumnarSourceOp::consumeReadyReader(ColumnarReaderPtr reader)
{
    assert(current_reader_work);
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
    state = ColumnarSourceState::READING;
}

OperatorStatus RNColumnarSourceOp::awaitImpl()
{
    switch (state)
    {
    case ColumnarSourceState::DONE:
    case ColumnarSourceState::READY_BLOCK:
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarSourceState::READING:
        // Have an input stream ready — go read a block in executeIOImpl.
        return OperatorStatus::IO_IN;
    case ColumnarSourceState::WAIT_READER:
    {
        // We are waiting for a prefetch thread to finish materializing
        // current_reader_work. Check the state without blocking.
        assert(current_reader_work);
        const auto region_id = current_reader_work->plan.region_id;
        LOG_INFO(log, "[src={}] WAIT_READER re-check, region_id={}", fmt::ptr(this), region_id);
        std::optional<ColumnarReaderPtr> taken_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                LOG_INFO(log, "[src={}] WAIT_READER -> Ready, region_id={}", fmt::ptr(this), region_id);
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
                break;
            case RNColumnarReaderMaterializeState::Failed:
                LOG_INFO(log, "[src={}] WAIT_READER -> Failed, region_id={}", fmt::ptr(this), region_id);
                std::rethrow_exception(current_reader_work->exception);
            case RNColumnarReaderMaterializeState::Consumed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "columnar reader work for region {} is already consumed",
                    region_id);
            case RNColumnarReaderMaterializeState::Creating:
                // Prefetch hasn't finished yet — return IO_IN so executeIOImpl
                // can either consume the reader (if Ready by then) or materialize
                // inline. Avoids the lost wakeup race of WAIT_FOR_NOTIFY.
                LOG_INFO(
                    log,
                    "[src={}] WAIT_READER -> still Creating, return IO_IN, region_id={}",
                    fmt::ptr(this),
                    region_id);
                return OperatorStatus::IO_IN;
            case RNColumnarReaderMaterializeState::NotStarted:
                // Safety net: an acquired work should never still be NotStarted
                // when we are in WAIT_READER state. Fall through to IO_IN so
                // executeIOImpl can materialize it inline.
                LOG_INFO(
                    log,
                    "[src={}] WAIT_READER -> NotStarted (fallback to inline), region_id={}",
                    fmt::ptr(this),
                    region_id);
                current_reader_work->state = RNColumnarReaderMaterializeState::Creating;
                return OperatorStatus::IO_IN;
            }
        }
        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
            return OperatorStatus::IO_IN;
        }
        return OperatorStatus::IO_IN; // unreachable
    }
    case ColumnarSourceState::NEED_READER:
    {
        // Acquire the next reader work.
        auto next_work = task->tryAcquireReaderWork();
        if (!next_work.has_value())
        {
            LOG_INFO(log, "[src={}] NEED_READER -> DONE (no more works)", fmt::ptr(this));
            state = ColumnarSourceState::DONE;
            return OperatorStatus::HAS_OUTPUT;
        }
        current_reader_work = std::move(next_work.value());
        const auto region_id = current_reader_work->plan.region_id;

        // Single lock acquisition to atomically check state and decide next action.
        std::optional<ColumnarReaderPtr> taken_reader;
        bool should_materialize = false;  // inline materialize in IO pool
        {
            std::lock_guard lock(current_reader_work->mutex);
            switch (current_reader_work->state)
            {
            case RNColumnarReaderMaterializeState::Ready:
                LOG_INFO(log, "[src={}] NEED_READER acquired Ready, region_id={}", fmt::ptr(this), region_id);
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
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
                    region_id);
            case RNColumnarReaderMaterializeState::NotStarted:
            case RNColumnarReaderMaterializeState::Creating:
                // Whether the reader work hasn't been started yet or a prefetch
                // thread is already working on it, return IO_IN so executeIOImpl
                // handles it inline in the IO pool. This avoids the lost wakeup
                // race that occurs when a detached thread's one-shot notifyAll
                // executes before the scheduler registers the waiting task.
                LOG_INFO(
                    log,
                    "[src={}] NEED_READER acquired {} -> inline, region_id={}",
                    fmt::ptr(this),
                    current_reader_work->state == RNColumnarReaderMaterializeState::NotStarted
                        ? "NotStarted" : "Creating",
                    region_id);
                current_reader_work->state = RNColumnarReaderMaterializeState::Creating;
                should_materialize = true;
                break;
            }
        }

        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
            return OperatorStatus::IO_IN;
        }
        if (should_materialize)
        {
            // executeIOImpl will materialize the reader inline in the IO pool.
            return OperatorStatus::IO_IN;
        }
        return OperatorStatus::IO_IN; // unreachable
    }
    }

    return OperatorStatus::IO_IN; // unreachable
}

OperatorStatus RNColumnarSourceOp::executeIOImpl()
{
    switch (state)
    {
    case ColumnarSourceState::DONE:
    case ColumnarSourceState::READY_BLOCK:
        return OperatorStatus::HAS_OUTPUT;
    case ColumnarSourceState::NEED_READER:
    case ColumnarSourceState::WAIT_READER:
    {
        // awaitImpl has set current_reader_work to Creating and returned IO_IN.
        // Check if prefetch already finished (Ready) — if so, consume directly.
        // Otherwise materialize inline in the IO pool.
        assert(current_reader_work);
        const auto region_id = current_reader_work->plan.region_id;

        std::optional<ColumnarReaderPtr> taken_reader;
        {
            std::lock_guard lock(current_reader_work->mutex);
            if (current_reader_work->state == RNColumnarReaderMaterializeState::Ready)
            {
                // Prefetch finished before we got scheduled on IO pool — consume directly.
                LOG_INFO(
                    log,
                    "[src={}] executeIO found ready (prefetch beat us), region_id={}",
                    fmt::ptr(this),
                    region_id);
                taken_reader.emplace(std::move(current_reader_work->reader.value()));
                current_reader_work->reader.reset();
                current_reader_work->exception = nullptr;
                current_reader_work->state = RNColumnarReaderMaterializeState::Consumed;
            }
        }

        if (taken_reader.has_value())
        {
            consumeReadyReader(std::move(taken_reader.value()));
        }
        else
        {
            // Still Creating (or NotStarted from safety net) — materialize inline.
            LOG_INFO(
                log,
                "[src={}] executeIO materializing inline, region_id={}",
                fmt::ptr(this),
                region_id);

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
            LOG_INFO(log, "[src={}] inline materialize done, region_id={}", fmt::ptr(this), region_id);

            consumeReadyReader(std::move(reader));
        }
        // fall through to READING
    }
    case ColumnarSourceState::READING:
    {
        assert(current_input_stream);
        // Read exactly one block.
        FilterPtr filter_ignored = nullptr;
        Stopwatch w{CLOCK_MONOTONIC_COARSE};
        Block block = current_input_stream->read(filter_ignored, false);
        duration_read_sec += w.elapsedSeconds();
        if likely (block && block.rows() > 0)
        {
            total_rows += block.rows();
            total_bytes += block.bytes();
            t_block.emplace(std::move(block));
            state = ColumnarSourceState::READY_BLOCK;
            LOG_DEBUG(log, "[src={}] read block rows={} bytes={}", fmt::ptr(this), t_block->rows(), t_block->bytes());
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            // This reader work is exhausted. Release it and loop to acquire the next.
            LOG_INFO(log, "[src={}] reader exhausted, total_streams={}", fmt::ptr(this), total_streams);
            current_input_stream.reset();
            state = ColumnarSourceState::NEED_READER;
            return awaitImpl();
        }
    }
    }

    return OperatorStatus::HAS_OUTPUT; // unreachable
}

} // namespace DB
#endif
