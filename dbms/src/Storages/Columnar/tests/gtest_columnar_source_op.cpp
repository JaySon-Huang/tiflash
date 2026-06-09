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

#include <Flash/Executor/PipelineExecutorContext.h>
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
    // read() delegates to awaitImpl() and returns IO_IN when no reader is available yet.
    EXPECT_EQ(source->read(block), OperatorStatus::IO_IN);

    // executeIO() finds no reader index, marks source done, and returns HAS_OUTPUT.
    EXPECT_EQ(source->executeIO(), OperatorStatus::HAS_OUTPUT);

    // After EOF, source must still emit one empty block for downstream operators.
    EXPECT_EQ(source->read(block), OperatorStatus::HAS_OUTPUT);
    EXPECT_EQ(block.rows(), 0);

    source->operateSuffix();
}
} // namespace DB::tests

#endif
