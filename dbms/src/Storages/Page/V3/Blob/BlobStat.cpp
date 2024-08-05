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

#include <Common/ProfileEvents.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/Page/V3/Blob/BlobStat.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/PathPool.h>
#include <boost_wrapper/string_split.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>

#pragma GCC diagnostic push
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
// include to suppress warnings on NO_THREAD_SAFETY_ANALYSIS. clang can't work without this include, don't know why
#include <grpcpp/security/credentials.h>
#pragma GCC diagnostic pop

namespace ProfileEvents
{
extern const Event PSMWritePages;
extern const Event PSMReadPages;
extern const Event PSV3MBlobExpansion;
extern const Event PSV3MBlobReused;
} // namespace ProfileEvents

namespace DB::PS::V3
{

/**********************
  * BlobStats methods *
  *********************/

BlobStats::BlobStats(LoggerPtr log_, PSDiskDelegatorPtr delegator_, BlobConfig & config_)
    : log(std::move(log_))
    , delegator(delegator_)
    , config(config_)
{}

std::tuple<bool, String> BlobStats::restoreByEntry(const PageEntryV3 & entry)
{
    if (entry.file_id != INVALID_BLOBFILE_ID)
    {
        auto stat = blobIdToStat(entry.file_id);
        return stat->restoreSpaceMap(entry.offset, entry.getTotalSize());
    }
    else
    {
        // It must be an entry point to remote data location
        RUNTIME_CHECK(entry.checkpoint_info.is_valid && entry.checkpoint_info.is_local_data_reclaimed);
        return std::make_tuple(true, "");
    }
}

std::pair<BlobFileId, String> BlobStats::getBlobIdFromName(const String & blob_name)
{
    String err_msg;
    if (!startsWith(blob_name, BlobFile::BLOB_PREFIX_NAME))
    {
        return {INVALID_BLOBFILE_ID, err_msg};
    }

    Strings ss;
    boost::split(ss, blob_name, boost::is_any_of("_"));

    if (ss.size() != 2)
    {
        return {INVALID_BLOBFILE_ID, err_msg};
    }

    try
    {
        const auto & blob_id = std::stoull(ss[1]);
        return {blob_id, err_msg};
    }
    catch (std::invalid_argument & e)
    {
        err_msg = e.what();
    }
    catch (std::out_of_range & e)
    {
        err_msg = e.what();
    }
    return {INVALID_BLOBFILE_ID, err_msg};
}

void BlobStats::restore()
{
    for (auto & [path, stats] : stats_map)
    {
        (void)path;
        for (const auto & stat : stats)
        {
            stat->recalculateSpaceMap();
            cur_max_id = std::max(stat->id, cur_max_id);
        }
    }
}

std::lock_guard<std::mutex> BlobStats::lock() const NO_THREAD_SAFETY_ANALYSIS
{
    return std::lock_guard(lock_stats);
}

BlobStats::BlobStatPtr BlobStats::createStat(
    BlobFileId blob_file_id,
    UInt64 max_caps,
    const std::lock_guard<std::mutex> & guard)
{
    for (auto & [path, stats] : stats_map)
    {
        (void)path;
        for (const auto & stat : stats)
        {
            if (stat->id == blob_file_id)
            {
                throw Exception(
                    fmt::format("BlobStats can not create, blob_id is exist, blob_id={}", blob_file_id),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    // Create a stat without checking the file_id exist or not
    return createStatNotChecking(blob_file_id, max_caps, guard);
}

BlobStats::BlobStatPtr BlobStats::createStatNotChecking(
    BlobFileId blob_file_id,
    UInt64 max_caps,
    const std::lock_guard<std::mutex> &)
{
    LOG_INFO(log, "Created a new BlobStat, blob_id={} capacity={}", blob_file_id, max_caps);
    // Only BlobFile which total capacity is smaller or equal to config.file_limit_size can be reused for another write
    auto stat_type
        = max_caps <= config.file_limit_size ? BlobStats::BlobStatType::NORMAL : BlobStats::BlobStatType::READ_ONLY;
    BlobStatPtr stat = std::make_shared<BlobStat>(
        blob_file_id,
        static_cast<SpaceMap::SpaceMapType>(config.spacemap_type.get()),
        max_caps,
        stat_type);

    PageFileIdAndLevel id_lvl{blob_file_id, 0};
    auto path = delegator->choosePath(id_lvl);
    /// This function may be called when restoring an old BlobFile at restart or creating a new BlobFile.
    /// If restoring an old BlobFile, the BlobFile path maybe already added to delegator, but an another call to `addPageFileUsedSize` should do no harm.
    /// If creating a new BlobFile, we need to register the BlobFile's path to delegator, so it's necessary to call `addPageFileUsedSize` here.
    delegator->addPageFileUsedSize({blob_file_id, 0}, 0, path, true);
    stats_map[path].emplace_back(stat);
    return stat;
}

void BlobStats::eraseStat(const BlobStatPtr && stat)
{
    auto stats_lock = lock();
    eraseStatImpl(std::move(stat), stats_lock);
}

void BlobStats::eraseStatImpl(const BlobStatPtr && stat, const std::lock_guard<std::mutex> &)
{
    PageFileIdAndLevel id_lvl{stat->id, 0};
    stats_map[delegator->getPageFilePath(id_lvl)].remove(stat);
}

void BlobStats::setAllToReadOnly() NO_THREAD_SAFETY_ANALYSIS
{
    auto lock_stats = lock();
    for (const auto & [path, stats] : stats_map)
    {
        UNUSED(path);
        for (const auto & stat : stats)
        {
            LOG_INFO(log, "BlobStat is set to read only, blob_id={}", stat->id);
            stat->changeToReadOnly();
        }
    }
}

[[nodiscard]] std::tuple<std::unique_lock<std::mutex>, BlobStats::BlobStatPtr> //
BlobStats::lockStatForInsert(size_t length, PageType page_type) NO_THREAD_SAFETY_ANALYSIS
{
    auto lock_stats = lock();
    BlobFileId blob_file_id = INVALID_BLOBFILE_ID;
    BlobStatPtr stat;
    std::tie(stat, blob_file_id) = chooseStat(length, page_type, lock_stats);
    if (stat == nullptr)
    {
        // No valid stat for putting data with `length`, create a new one
        stat = createStat(blob_file_id, std::max(length, config.file_limit_size.get()), lock_stats);
    }

    // We must get the lock from BlobStat under the BlobStats lock
    // to ensure that BlobStat updates are serialized.
    // Otherwise it may cause stat to fail to get the span for writing
    // and throwing exception.
    auto lock_on_stat = stat->lock();
    return {std::move(lock_on_stat), std::move(stat)};
}

std::pair<BlobStats::BlobStatPtr, BlobFileId> BlobStats::chooseStat(
    size_t buf_size,
    PageType page_type,
    const std::lock_guard<std::mutex> &)
{
    // No stats exist
    if (stats_map.empty())
    {
        auto next_id = PageTypeUtils::nextFileID(page_type, cur_max_id);
        cur_max_id = next_id;
        return std::make_pair(nullptr, next_id);
    }

    // If the stats_map size changes, or stats_map_path_index is out of range,
    // then make stats_map_path_index fit to current size.
    stats_map_path_index %= stats_map.size();

    auto stats_iter = stats_map.begin();
    std::advance(stats_iter, stats_map_path_index);

    size_t path_iter_idx = 0;
    for (path_iter_idx = 0; path_iter_idx < stats_map.size(); ++path_iter_idx)
    {
        // Try to find a suitable stat under current path (path=`stats_iter->first`)
        for (const auto & stat : stats_iter->second)
        {
            if (PageTypeUtils::getPageType(stat->id) != page_type)
                continue;

            auto defer_lock = stat->defer_lock();
            if (defer_lock.try_lock() && stat->isNormal() && stat->sm_max_caps >= buf_size)
            {
                return std::make_pair(stat, INVALID_BLOBFILE_ID);
            }
        }

        // Try to find stat in the next path.
        stats_iter++;
        if (stats_iter == stats_map.end())
        {
            stats_iter = stats_map.begin();
        }
    }

    // advance the `stats_map_path_idx` without size checking
    stats_map_path_index += path_iter_idx + 1;

    // Can not find a suitable stat under all paths
    auto next_id = PageTypeUtils::nextFileID(page_type, cur_max_id);
    cur_max_id = next_id;
    return std::make_pair(nullptr, next_id);
}

BlobStats::BlobStatPtr BlobStats::blobIdToStat(BlobFileId file_id, bool ignore_not_exist) NO_THREAD_SAFETY_ANALYSIS
{
    auto guard = lock();
    for (const auto & [path, stats] : stats_map)
    {
        (void)path;
        for (const auto & stat : stats)
        {
            if (stat->id == file_id)
            {
                return stat;
            }
        }
    }

    if (!ignore_not_exist)
    {
        throw Exception(fmt::format("Can't find BlobStat with [blob_id={}]", file_id), ErrorCodes::LOGICAL_ERROR);
    }

    return nullptr;
}

BlobStats::StatsMap BlobStats::getStats() const NO_THREAD_SAFETY_ANALYSIS
{
    auto guard = lock();
    return stats_map;
}

/*********************
  * BlobStat methods *
  ********************/

BlobFileOffset BlobStats::BlobStat::getPosFromStat(size_t buf_size, const std::unique_lock<std::mutex> &)
{
    // A shortcut for empty page. All empty pages will be stored
    // at the beginning of the BlobFile. It should not affects the
    // sm_max_caps or other fields by adding these empty pages.
    if (unlikely(buf_size == 0))
        return 0;

    BlobFileOffset offset = 0;
    UInt64 max_cap = 0;
    bool expansion = true;

    // We need to assume that this insert will reduce max_cap.
    const auto old_max_cap = sm_max_caps;
    assert(sm_max_caps >= buf_size);

    std::tie(offset, max_cap, expansion) = smap->searchInsertOffset(buf_size);
    ProfileEvents::increment(expansion ? ProfileEvents::PSV3MBlobExpansion : ProfileEvents::PSV3MBlobReused);

    // Whatever `searchInsertOffset` success or failed, Max capability still need update.
    // Other threads may also be waiting on the lock on the same `stat::getPosFromStat` concurrently.
    // If max_cap is not updated, it may make the same blob_id accept multiple buffers and exceed its max_cap.
    sm_max_caps = max_cap;

    // Can't insert into this spacemap
    if (unlikely(offset == INVALID_BLOBFILE_OFFSET))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Get postion from BlobStat failed, it may caused by `sm_max_caps` is no correct. size={} "
            "old_max_caps={} max_caps={} blob_id={}",
            buf_size,
            old_max_cap,
            max_cap,
            id);
    }

    assert(offset != INVALID_BLOBFILE_OFFSET);
    if (offset + buf_size > sm_total_size)
    {
        // This file must be expanded
        auto expand_size = buf_size - (sm_total_size - offset);
        sm_total_size += expand_size;
        sm_valid_size += buf_size;
    }
    else
    {
        /**
         * The `offset` reuses the original address. 
         * Current blob file is not expanded.
         * Only update valid size.
         */
        sm_valid_size += buf_size;
    }

    sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
    return offset;
}

size_t BlobStats::BlobStat::removePosFromStat(BlobFileOffset offset, size_t buf_size)
{
    auto stat_lock = lock();
    if (!smap->markFree(offset, buf_size))
    {
        LOG_ERROR(Logger::get(), smap->toDebugString());
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Remove position from BlobStat failed, invalid position, offset={} buf_size={} blob_id={}",
            offset,
            buf_size,
            id);
    }

    sm_valid_size -= buf_size;
    sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
    return sm_valid_size;
}

std::tuple<bool, String> BlobStats::BlobStat::restoreSpaceMap(BlobFileOffset offset, size_t buf_size)
{
    bool success = smap->markUsed(offset, buf_size);
    if (!success)
    {
        String msg = (buf_size == 0) ? "" : smap->toDebugString();
        return std::make_tuple(success, msg);
    }
    return std::make_tuple(success, "");
}

void BlobStats::BlobStat::recalculateSpaceMap()
{
    const auto & [total_size, valid_size] = smap->getSizes();
    sm_total_size = total_size;
    sm_valid_size = valid_size;
    sm_valid_rate = total_size == 0 ? 0.0 : valid_size * 1.0 / total_size;
    recalculateCapacity();
}

void BlobStats::BlobStat::recalculateCapacity()
{
    sm_max_caps = smap->updateAccurateMaxCapacity();
}
} // namespace DB::PS::V3
