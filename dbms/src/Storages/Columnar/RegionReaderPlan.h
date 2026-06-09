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

#include <Core/Types.h>
#include <Storages/Columnar/RNProxyReaderPlan.h>
#include <Storages/KVStore/Types.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/RegionCache.h>

#include <tuple>
#include <unordered_map>
#include <vector>

namespace DB
{

class Context;

using ColumnarPhysicalTableRanges = std::vector<std::tuple<TableID, pingcap::coprocessor::KeyRanges>>;
using ColumnarBucketSplitUnit = std::pair<TableID, pingcap::coprocessor::KeyRange>;

struct ColumnarRegionReaderPlan
{
    RegionID region_id;
    pingcap::kv::RegionVerID region_ver_id;
    // In columnar, one Region could contain multiple physical table ranges.
    ColumnarPhysicalTableRanges physical_table_ranges;
    std::vector<ColumnarBucketSplitUnit> bucket_units;
};

bool isBucketBoundaryInsideRange(const String & bucket_key, const pingcap::coprocessor::KeyRange & range);

// Return <has_bucket_split, bucket_split_units>. bucket_split_units is only valid when has_bucket_split is true.
std::tuple<bool, std::vector<ColumnarBucketSplitUnit>> splitRangesByBucketKeys(
    const ColumnarPhysicalTableRanges & physical_table_ranges,
    const std::vector<String> & bucket_keys);

struct BuildColumnarRegionReaderPlansOutput
{
    std::vector<ColumnarRegionReaderPlan> region_reader_plans;
    size_t planned_reader_num = 0;
    size_t total_split_bucket_num = 0;
};
BuildColumnarRegionReaderPlansOutput buildColumnarRegionReaderPlans(
    const Context & context,
    const std::unordered_map<RegionID, ColumnarPhysicalTableRanges> & all_remote_regions_by_region,
    const std::unordered_map<RegionID, pingcap::kv::RegionVerID> & region_ver_ids,
    bool enable_bucket_parallel);

std::vector<RNProxyReaderPlan> flattenColumnarRegionReaderPlans(
    const std::vector<ColumnarRegionReaderPlan> & region_reader_plans);

} // namespace DB

#endif
