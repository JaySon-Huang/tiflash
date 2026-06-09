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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context_fwd.h>
#include <Operators/Operator.h>
#include <Storages/Columnar/ColumnarReaderSlot.h>

#include <memory>

namespace DB
{
class RNProxyReadTask;
using RNProxyReadTaskPtr = std::shared_ptr<RNProxyReadTask>;

/// Phase 4 source states (docs/design/2026-06-09-storage-disaggregated-columnar-pipeline.md).
enum class RNProxySourceState
{
    /// All reader indices consumed; readImpl emits one final empty block.
    Done,
    /// t_block is cached; readImpl emits without proxy FFI / await / IO.
    ReadyBlock,
    /// current_input_stream is attached; executeIOImpl reads at most one block.
    Reading,
    /// current_reader_idx is reserved while prefetch materializes the slot.
    WaitReader,
    /// Claim the next reader index from the shared task pool.
    AcquireReader,
};

/// Pipeline source operator for disaggregated columnar read through proxy FFI.
///
/// Each concurrent source pulls reader indices from a shared RNProxyReadTask pool,
/// reads one block per IO task, and hands blocks to downstream transforms
/// (generated column placeholder, cast, filter, projection).
///
/// Scheduling model (see docs/design/2026-06-09-storage-disaggregated-columnar-pipeline.md):
/// - readImpl(): CPU path only; emits ReadyBlock / Done, otherwise schedules IO or notify
/// - awaitImpl(): non-blocking slot check; never calls proxy FFI or allocates blocks
/// - executeIOImpl(): attaches stream if needed, reads at most one block per call
/// - operateSuffixImpl()/destructor: unregister from shared RNProxyReadTask; cancel if not Done
///
/// Stream-model reads still go through RNProxyInputStream in StorageDisaggregatedColumnar.cpp.
class RNProxySourceOp : public SourceOp
{
    static constexpr auto NAME = "RNProxy";

public:
    struct Options
    {
        PipelineExecutorContext & exec_context;
        RNProxyReadTaskPtr task;
    };

    explicit RNProxySourceOp(const Options & options);

    ~RNProxySourceOp() override;

    static SourceOpPtr create(const Options & options) { return std::make_unique<RNProxySourceOp>(options); }

    String getName() const override { return NAME; }

    IOProfileInfoPtr getIOProfileInfo() const override { return IOProfileInfo::createForLocal(profile_info_ptr); }

#ifdef DBMS_PUBLIC_GTEST
    RNProxySourceState getSourceStateForGTest() const { return source_state; }
#endif

protected:
    void operateSuffixImpl() override;

    void operatePrefixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    OperatorStatus executeIOImpl() override;

private:
    /// Lightweight scheduling on CPU / wait-reactor threads. No proxy FFI or Block allocation.
    OperatorStatus scheduleNextAction();

    OperatorStatus scheduleWaitReader();

    OperatorStatus scheduleAcquireReader();

    void releaseCurrentReader();

    void attachInputStreamForCurrentReader();

    /// Phase 5: unregister from shared RNProxyReadTask once; cancel if this source did not reach Done.
    void cleanupSharedTaskIfNeeded();

    const Context & context;
    const LoggerPtr log;
    RNProxyReadTaskPtr task;
    UInt64 total_bytes = 0;
    size_t total_rows = 0;
    size_t total_streams = 0;

    RNProxySourceState source_state = RNProxySourceState::AcquireReader;

    std::optional<size_t> current_reader_idx;
    std::shared_ptr<RNProxyReaderSlot> current_reader_slot;
    BlockInputStreamPtr current_input_stream;

    // Block read from the current reader stream; emitted on the next readImpl() call.
    std::optional<Block> t_block = std::nullopt;

    Stopwatch total_cost_watch{CLOCK_MONOTONIC_COARSE};

    double duration_read_sec = 0;

    bool shared_task_cleanup_done = false;
};
} // namespace DB
#endif
