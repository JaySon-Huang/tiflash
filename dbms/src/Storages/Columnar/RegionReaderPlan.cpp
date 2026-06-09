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

#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/Columnar/RegionReaderPlan.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace FailPoints
{
extern const char force_return_columnar_region_bucket_keys[];
} // namespace FailPoints

namespace
{

std::vector<String> getRegionBucketKeysFromProxy(const Context & context, RegionID region_id, UInt64 region_ver)
{
    using BucketKeysByRegion = std::unordered_map<RegionID, std::vector<String>>;
    fiu_do_on(FailPoints::force_return_columnar_region_bucket_keys, {
        if (auto v = FailPointHelper::getFailPointVal(FailPoints::force_return_columnar_region_bucket_keys); v)
        {
            const auto & bucket_keys_by_region
                = std::any_cast<const BucketKeysByRegion &>(v.value());
            if (auto it = bucket_keys_by_region.find(region_id); it != bucket_keys_by_region.end())
                return it->second;
            return {};
        }
    });

    const Context & global_ctx = context.getGlobalContext();
    const TiFlashRaftProxyHelper * proxy_helper = global_ctx.getSharedContextDisagg()->getColumnarProxyHelper();
    if (proxy_helper == nullptr || proxy_helper->cloud_storage_engine_interfaces.fn_get_region_bucket_keys == nullptr)
    {
        // Treat as no bucket keys
        return {};
    }

    RustStrWithViewVec bucket_keys = proxy_helper->cloud_storage_engine_interfaces.fn_get_region_bucket_keys(
        region_id,
        region_ver,
        proxy_helper->proxy_ptr);
    SCOPE_EXIT({
        if (bucket_keys.inner.ptr != nullptr)
            RustGcHelper::instance().gcRustPtr(bucket_keys.inner.ptr, bucket_keys.inner.type);
    });

    std::vector<String> res;
    res.reserve(static_cast<size_t>(bucket_keys.len));
    for (size_t i = 0; i < bucket_keys.len; ++i)
        res.emplace_back(bucket_keys.buffs[i].data, bucket_keys.buffs[i].len);
    return res;
}

} // namespace

bool isBucketBoundaryInsideRange(const String & bucket_key, const pingcap::coprocessor::KeyRange & range)
{
    if (bucket_key.empty())
        return false;
    if (!range.start_key.empty() && bucket_key <= range.start_key)
        return false;
    if (!range.end_key.empty() && bucket_key >= range.end_key)
        return false;
    return true;
}

std::tuple<bool, std::vector<ColumnarBucketSplitUnit>> splitRangesByBucketKeys(
    const ColumnarPhysicalTableRanges & physical_table_ranges,
    const std::vector<String> & bucket_keys)
{
    bool has_bucket_split = false;
    std::vector<ColumnarBucketSplitUnit> units;

    // less than or equal to 2 bucket keys means no effective split, since the bucket keys only serve as boundaries.
    if (bucket_keys.size() <= 2)
        return {has_bucket_split, std::move(units)};

    for (const auto & [table_id, ranges] : physical_table_ranges)
    {
        for (const auto & range : ranges)
        {
            String current_start = range.start_key;
            bool current_range_split = false;
            for (const auto & bucket_key : bucket_keys)
            {
                const auto decoded_bucket_key
                    = RecordKVFormat::decodeTiKVKey(TiKVKey(bucket_key.data(), bucket_key.size()));
                String normalized_bucket_key(decoded_bucket_key.data(), decoded_bucket_key.size());
                // skip if bucket key is not strictly inside (range.start_key, range.end_key)
                if (!isBucketBoundaryInsideRange(normalized_bucket_key, range))
                    continue;
                // split the range by bucket key
                units.emplace_back(
                    table_id,
                    pingcap::coprocessor::KeyRange{current_start, normalized_bucket_key});
                current_start = std::move(normalized_bucket_key);
                current_range_split = true;
            }
            // skip if the current start is not less than the range end
            if (!range.end_key.empty() && current_start >= range.end_key)
                continue;
            // add the remaining range after the last bucket key
            units.emplace_back(table_id, pingcap::coprocessor::KeyRange{current_start, range.end_key});
            has_bucket_split = has_bucket_split || current_range_split;
        }
    }
    return {has_bucket_split, std::move(units)};
}

BuildColumnarRegionReaderPlansOutput buildColumnarRegionReaderPlans(
    const Context & context,
    const std::unordered_map<RegionID, ColumnarPhysicalTableRanges> & all_remote_regions_by_region,
    const std::unordered_map<RegionID, pingcap::kv::RegionVerID> & region_ver_ids,
    bool enable_bucket_parallel)
{
    BuildColumnarRegionReaderPlansOutput output;
    size_t region_num = all_remote_regions_by_region.size();
    output.planned_reader_num = region_num;

    output.region_reader_plans.reserve(region_num);
    for (const auto & [region_id, physical_table_ranges] : all_remote_regions_by_region)
    {
        ColumnarRegionReaderPlan plan{
            .region_id = region_id,
            .region_ver_id = region_ver_ids.at(region_id),
            .physical_table_ranges = physical_table_ranges,
        };
        if (enable_bucket_parallel)
        {
            auto bucket_keys = getRegionBucketKeysFromProxy(context, region_id, plan.region_ver_id.ver);
            auto [has_bucket_split, units] = splitRangesByBucketKeys(physical_table_ranges, bucket_keys);
            if (has_bucket_split && units.size() > 1)
            {
                output.planned_reader_num += units.size() - 1;
                output.total_split_bucket_num += units.size();
                plan.bucket_units = std::move(units);
            }
        }
        output.region_reader_plans.emplace_back(std::move(plan));
    }
    return output;
}

std::vector<RNProxyReaderPlan> flattenColumnarRegionReaderPlans(
    const std::vector<ColumnarRegionReaderPlan> & region_reader_plans)
{
    std::vector<RNProxyReaderPlan> flattened_reader_plans;
    for (const auto & plan : region_reader_plans)
    {
        if (plan.bucket_units.empty())
        {
            flattened_reader_plans.push_back(RNProxyReaderPlan{
                .region_id = plan.region_id,
                .region_ver = plan.region_ver_id.ver,
                .region_conf_ver = plan.region_ver_id.conf_ver,
                .physical_table_ranges = plan.physical_table_ranges,
            });
        }
        else
        {
            for (const auto & [table_id, range] : plan.bucket_units)
            {
                flattened_reader_plans.push_back(RNProxyReaderPlan{
                    .region_id = plan.region_id,
                    .region_ver = plan.region_ver_id.ver,
                    .region_conf_ver = plan.region_ver_id.conf_ver,
                    .physical_table_ranges = ColumnarPhysicalTableRanges{
                        std::make_tuple(table_id, pingcap::coprocessor::KeyRanges{range})},
                });
            }
        }
    }
    return flattened_reader_plans;
}

} // namespace DB

#endif
