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

#include <Common/config.h>
#if ENABLE_NEXT_GEN_COLUMNAR

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/NullSourceOp.h>
#include <Storages/Columnar/ColumnarReaderSlot.h>
#include <Storages/Columnar/ColumnarSourceOp.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
class ColumnarSourceOpTest : public ::testing::Test
{
protected:
    void SetUp() override { context = TiFlashTestEnv::getContext(); }

    Block buildNullSourceHeaderFromTask(const RNProxyReadTaskPtr & task) const
    {
        return AddExtraTableIDColumnTransformAction::buildHeader(
            task->getColumnsToRead(),
            task->getExtraTableIDIndex());
    }

    ContextPtr context;
};

TEST_F(ColumnarSourceOpTest, ReaderSlotDefaultState)
{
    auto slot = std::make_shared<RNProxyReaderSlot>();
    EXPECT_EQ(slot->state, RNProxyReaderMaterializeState::NotStarted);
    EXPECT_FALSE(slot->reader.has_value());
    EXPECT_FALSE(slot->exception);
}

TEST_F(ColumnarSourceOpTest, EmptyTaskHelper)
{
    auto task = createEmptyRNProxyReadTaskForGTest(*context);
    EXPECT_EQ(task->getReaderCount(), 0);
    EXPECT_EQ(task->getColumnsToRead().size(), 1);
    EXPECT_EQ(task->getExtraTableIDIndex(), MutSup::invalid_col_id);
}

TEST_F(ColumnarSourceOpTest, SourceOpMetadata)
{
    PipelineExecutorContext exec_context;
    auto task = createEmptyRNProxyReadTaskForGTest(*context);
    ASSERT_EQ(task->getColumnsToRead().size(), 1);
    auto source = RNProxySourceOp::create({
        .exec_context = exec_context,
        .task = task,
    });

    EXPECT_EQ(source->getName(), "RNProxy");
    EXPECT_TRUE(source->getIOProfileInfo() != nullptr);
    EXPECT_EQ(task->getReaderCount(), 0);
}

TEST_F(ColumnarSourceOpTest, EmptyReaderTaskEOF)
{
    PipelineExecutorContext exec_context;
    auto task = createEmptyRNProxyReadTaskForGTest(*context);
    auto source = RNProxySourceOp::create({
        .exec_context = exec_context,
        .task = task,
    });

    source->operatePrefix();

    Block block;
    // Zero readers: awaitImpl marks source done and returns HAS_OUTPUT without blocking IO.
    EXPECT_EQ(source->read(block), OperatorStatus::HAS_OUTPUT);
    EXPECT_EQ(block.rows(), 0);

    source->operateSuffix();
}

TEST_F(ColumnarSourceOpTest, TryTakeReadyReaderReturnsNullWhenNotReady)
{
    auto task = createRNProxyReadTaskWithReaderPlansForGTest(*context, 1);
    EXPECT_FALSE(task->tryTakeReadyReader(0).has_value());
    EXPECT_EQ(task->getReaderMaterializeState(0), RNProxyReaderMaterializeState::NotStarted);

    setReaderSlotStateForGTest(task, 0, RNProxyReaderMaterializeState::Creating);
    EXPECT_FALSE(task->tryTakeReadyReader(0).has_value());
    EXPECT_EQ(task->getReaderMaterializeState(0), RNProxyReaderMaterializeState::Creating);
}

TEST_F(ColumnarSourceOpTest, AwaitImplReturnsWaitForNotifyWhenReaderCreating)
{
    PipelineExecutorContext exec_context;
    auto task = createRNProxyReadTaskWithReaderPlansForGTest(*context, 1);
    setReaderSlotStateForGTest(task, 0, RNProxyReaderMaterializeState::Creating);
    auto source = RNProxySourceOp::create({
        .exec_context = exec_context,
        .task = task,
    });

    Block block;
    EXPECT_EQ(source->read(block), OperatorStatus::WAIT_FOR_NOTIFY);
}

TEST_F(ColumnarSourceOpTest, ReaderSlotIsNotifyFuture)
{
    auto slot = std::make_shared<RNProxyReaderSlot>();
    EXPECT_NE(dynamic_cast<NotifyFuture *>(slot.get()), nullptr);
}

TEST_F(ColumnarSourceOpTest, EmptyReaderUsesNullSourceInPipelineBuilder)
{
    PipelineExecutorContext exec_context;
    PipelineExecGroupBuilder group_builder;
    auto task = createEmptyRNProxyReadTaskForGTest(*context);
    const auto null_source_header = buildNullSourceHeaderFromTask(task);
    auto log = Logger::get("ColumnarSourceOpGTest");

    // Phase 2: zero reader count falls back to NullSourceOp (same header as RNProxySourceOp).
    group_builder.addConcurrency(std::make_unique<NullSourceOp>(exec_context, null_source_header, log->identifier()));

    ASSERT_FALSE(group_builder.empty());
    EXPECT_EQ(group_builder.concurrency(), 1);
    EXPECT_EQ(group_builder.getCurBuilder(0).source_op->getName(), "NullSourceOp");

    // Must succeed before generated column / cast / filter / projection are appended.
    const auto header = group_builder.getCurrentHeader();
    EXPECT_EQ(header.columns(), null_source_header.columns());
}

TEST_F(ColumnarSourceOpTest, TableScanProfileRecordedBeforeDownstreamTransforms)
{
    PipelineExecutorContext exec_context;
    PipelineExecGroupBuilder group_builder;
    auto task = createEmptyRNProxyReadTaskForGTest(*context);
    const auto table_scan_id = task->getExecutorID();
    const auto null_source_header = buildNullSourceHeaderFromTask(task);
    auto log = Logger::get("ColumnarSourceOpGTest");
    DAGContext dag_context(/*max_error_count=*/10);

    addColumnarPipelineSourcesAndRecordProfileForGTest(
        exec_context,
        group_builder,
        dag_context,
        table_scan_id,
        task,
        /*num_streams=*/4,
        null_source_header,
        log);

    const auto & inbound_io_map = dag_context.getInboundIOProfileInfosMap();
    const auto & operator_profile_map = dag_context.getOperatorProfileInfosMap();
    ASSERT_TRUE(inbound_io_map.contains(table_scan_id));
    ASSERT_TRUE(operator_profile_map.contains(table_scan_id));
    EXPECT_EQ(inbound_io_map.at(table_scan_id).size(), 1);
    EXPECT_EQ(operator_profile_map.at(table_scan_id).size(), 1);
    EXPECT_EQ(group_builder.getCurBuilder(0).source_op->getName(), "NullSourceOp");

    // addOperatorProfileInfos is first-write-wins for table_scan_id.
    const auto recorded_profile_ptr = operator_profile_map.at(table_scan_id)[0];
    group_builder.addConcurrency(std::make_unique<NullSourceOp>(exec_context, null_source_header, log->identifier()));
    dag_context.addOperatorProfileInfos(table_scan_id, group_builder.getCurProfileInfos());
    EXPECT_EQ(operator_profile_map.at(table_scan_id)[0], recorded_profile_ptr);
}
} // namespace DB::tests

#endif
