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

#include <Common/config.h>
#if ENABLE_NEXT_GEN_COLUMNAR
#include <Storages/Columnar/ColumnarSourceOp.h>
#include <Storages/StorageDisaggregatedColumnar.h>
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(StorageDisaggregatedColumnarTest, ReaderWorkStartsNotStarted)
{
    RNColumnarReaderPlan plan{
        .region_id = 1,
        .region_ver = 2,
        .region_conf_ver = 3,
        .physical_table_ranges = {},
    };

    RNColumnarReaderWork work(std::move(plan));

    // This baseline guards the source split: tests include both the new source header
    // and the shared reader-work contract it consumes.
    ASSERT_EQ(work.state, RNColumnarReaderMaterializeState::NotStarted);
    ASSERT_FALSE(work.reader.has_value());
    ASSERT_EQ(work.plan.region_id, 1);
}

} // namespace DB::tests
#endif
