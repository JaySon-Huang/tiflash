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

#pragma once

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Common/Logger.h>
#include <DataStreams/AddExtraTableIDColumnTransformAction.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Columnar/ColumnarReaderSlot.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>

#include <atomic>
#include <optional>
#include <string_view>
#pragma GCC diagnostic pop

namespace DB
{
class DAGContext;
class ThreadManager;

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
} // namespace DM

struct RNProxyReaderSharedContext;

struct RNProxyReaderPlan
{
    RegionID region_id;
    RegionVersion region_ver;
    UInt64 region_conf_ver;
    std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>> physical_table_ranges;
};

class RNProxyReadTask;
using RNProxyReadTaskPtr = std::shared_ptr<RNProxyReadTask>;
class RNProxyReadTask
    : public boost::noncopyable
    , public std::enable_shared_from_this<RNProxyReadTask>
{
public:
    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;

    static std::vector<RNProxyReadTaskPtr> buildProxyReadTaskWithBackoff(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    static std::vector<RNProxyReadTaskPtr> buildProxyReadTask(
        const LoggerPtr & log,
        const Context & context,
        UInt64 start_ts,
        const TiDBTableScan & table_scan,
        const FilterConditions & filter_conditions,
        const std::vector<RemoteTableRange> & remote_table_ranges,
        unsigned num_streams);

    BlockInputStreams getInputStreams();

    BlockInputStreamPtr createInputStream(size_t reader_index);

    BlockInputStreamPtr createInputStreamWithReader(size_t reader_index, ColumnarReaderPtr reader);

    ColumnarReaderPtr createColumnarReaderWithBackoff(size_t reader_index) const;

    ColumnarReaderPtr getOrCreateReader(size_t reader_index);

    /// Non-blocking pipeline helpers (Phase 3). Stream path keeps using getOrCreateReader().
    std::shared_ptr<RNProxyReaderSlot> getReaderSlot(size_t reader_index) const;

    RNProxyReaderMaterializeState getReaderMaterializeState(size_t reader_index) const;

    std::optional<ColumnarReaderPtr> tryTakeReadyReader(size_t reader_index);

    ColumnarReaderPtr materializeReaderInIOThread(size_t reader_index);

    void rethrowReaderSlotException(size_t reader_index) const;

    void prefetchReader(size_t reader_index);

    std::optional<size_t> tryAcquireReaderIndex();

    size_t getReaderCount() const;

    const Context & getContext() const;

    const LoggerPtr & getLog() const;

    const DM::ColumnDefines & getColumnsToRead() const;

    int getExtraTableIDIndex() const;

    TableID getLogicalTableID() const;

    const String & getExecutorID() const;

    RNProxyReadTask(
        std::vector<RNProxyReaderPlan> reader_plans,
        std::shared_ptr<RNProxyReaderSharedContext> shared_reader_context);

private:
    std::vector<RNProxyReaderPlan> reader_plans;
    std::shared_ptr<RNProxyReaderSharedContext> shared_reader_context;
    std::vector<std::shared_ptr<RNProxyReaderSlot>> reader_slots;
    std::atomic_size_t next_reader_index = 0;
    std::once_flag prefetch_thread_manager_once;
    std::shared_ptr<ThreadManager> prefetch_thread_manager;
};

class RNProxyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "RNProxy";

public:
    ~RNProxyInputStream();

    String getName() const { return NAME; }
    Block getHeader() const { return header; }
    void setHeader(const Block & header) { this->header = header; }
    Block read(FilterPtr & res_filter, bool return_filter);

protected:
    Block readImpl();
    Block readImpl(FilterPtr & res_filter, bool return_filter);

public:
    struct Options
    {
        const Context & context;
        LoggerPtr log;
        RNProxyReadTaskPtr task;
        size_t reader_index;
        const DM::ColumnDefines & columns_to_read;
        int extra_table_id_index;
        TableID table_id;
        const String & executor_id;
    };

    explicit RNProxyInputStream(const Options & options)
        : context(options.context)
        , log(options.log)
        , task(options.task)
        , reader_index(options.reader_index)
        , action(options.columns_to_read, options.extra_table_id_index)
        , table_id(options.table_id)
        , executor_id(options.executor_id)
    {
        // Keep header aligned with genNamesAndTypesForTableScan when TiDB requests _tidb_tid on partition scans.
        setHeader(action.getHeader());
    }

    static BlockInputStreamPtr create(const Options & options) { return std::make_shared<RNProxyInputStream>(options); }

private:
    friend class RNProxyReadTask;

    void setPreloadedReader(ColumnarReaderPtr reader);

    void ensureReader();

    const Context & context;
    const LoggerPtr log;
    RNProxyReadTaskPtr task;
    size_t reader_index;
    std::optional<ColumnarReaderPtr> reader;
    AddExtraTableIDColumnTransformAction action;
    TableID table_id;
    const String executor_id;
    Block header;

    bool done = false;

    double duration_deserialize_sec = 0;
    double duration_read_sec = 0;
    UInt64 batch_size = 10240;
    UInt64 total_bytes = 0;
};

#ifdef DBMS_PUBLIC_GTEST
/// Build an RNProxyReadTask with zero readers for pipeline source operator unit tests.
RNProxyReadTaskPtr createEmptyRNProxyReadTaskForGTest(const Context & context);

/// Build an RNProxyReadTask with `reader_count` placeholder reader plans for slot tests.
RNProxyReadTaskPtr createRNProxyReadTaskWithReaderPlansForGTest(const Context & context, size_t reader_count);

void setReaderSlotStateForGTest(
    const RNProxyReadTaskPtr & task,
    size_t reader_index,
    RNProxyReaderMaterializeState state);

/// Phase 2 unit test helper: mirrors addColumnarPipelineSourcesAndRecordProfile() in
/// StorageDisaggregatedColumnar.cpp (RNProxySourceOp / NullSourceOp + table scan profiles).
void addColumnarPipelineSourcesAndRecordProfileForGTest(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    DAGContext & dag_context,
    const String & table_scan_executor_id,
    RNProxyReadTaskPtr task_pool,
    unsigned num_streams,
    const Block & null_source_header,
    const LoggerPtr & log);
#endif
} // namespace DB
#endif
