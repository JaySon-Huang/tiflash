
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

#include <Common/Exception.h>
#include <IO/BaseFile/fwd.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>

#include <magic_enum.hpp>

namespace DB::DM
{

void DMFilePackFilter::init(ReadTag read_tag)
{
    Stopwatch watch;
    SCOPE_EXIT({ scan_context->total_rs_pack_filter_check_time_ns += watch.elapsed(); });
    size_t pack_count = dmfile->getPacks();
    auto read_all_packs = (rowkey_ranges.size() == 1 && rowkey_ranges[0].all()) || rowkey_ranges.empty();
    if (!read_all_packs)
    {
        tryLoadIndex(EXTRA_HANDLE_COLUMN_ID);
        std::vector<RSOperatorPtr> handle_filters;
        for (auto & rowkey_range : rowkey_ranges)
            handle_filters.emplace_back(toFilter(rowkey_range));
#ifndef NDEBUG
        // sanity check under debug mode to ensure the rowkey_range is correct common-handle or int64-handle
        if (!rowkey_ranges.empty())
        {
            bool is_common_handle = rowkey_ranges.begin()->is_common_handle;
            auto handle_col_type = dmfile->getColumnStat(EXTRA_HANDLE_COLUMN_ID).type;
            if (is_common_handle)
                RUNTIME_CHECK_MSG(
                    handle_col_type->getTypeId() == TypeIndex::String,
                    "handle_col_type_id={}",
                    magic_enum::enum_name(handle_col_type->getTypeId()));
            else
                RUNTIME_CHECK_MSG(
                    handle_col_type->getTypeId() == TypeIndex::Int64,
                    "handle_col_type_id={}",
                    magic_enum::enum_name(handle_col_type->getTypeId()));
            for (size_t i = 1; i < rowkey_ranges.size(); ++i)
            {
                RUNTIME_CHECK_MSG(
                    is_common_handle == rowkey_ranges[i].is_common_handle,
                    "i={} is_common_handle={} ith.is_common_handle={}",
                    i,
                    is_common_handle,
                    rowkey_ranges[i].is_common_handle);
            }
        }
#endif
        for (size_t i = 0; i < pack_count; ++i)
        {
            handle_res[i] = RSResult::None;
        }
        for (auto & handle_filter : handle_filters)
        {
            auto res = handle_filter->roughCheck(0, pack_count, param);
            std::transform(
                handle_res.begin(),
                handle_res.end(),
                res.begin(),
                handle_res.begin(),
                [](RSResult a, RSResult b) { return a || b; });
        }
    }

    ProfileEvents::increment(ProfileEvents::DMFileFilterNoFilter, pack_count);

    /// Check packs by handle_res
    pack_res = handle_res;
    auto after_pk = countUsePack();

    /// Check packs by read_packs
    if (read_packs)
    {
        for (size_t i = 0; i < pack_count; ++i)
        {
            pack_res[i] = read_packs->contains(i) ? pack_res[i] : RSResult::None;
        }
    }
    auto after_read_packs = countUsePack();
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftPKAndPackSet, after_read_packs);

    /// Check packs by filter in where clause
    if (filter)
    {
        // Load index based on filter.
        ColIds ids = filter->getColumnIDs();
        for (const auto & id : ids)
        {
            tryLoadIndex(id);
        }

        const auto check_results = filter->roughCheck(0, pack_count, param);
        std::transform(
            pack_res.cbegin(),
            pack_res.cend(),
            check_results.cbegin(),
            pack_res.begin(),
            [](RSResult a, RSResult b) { return a && b; });
    }
    else
    {
        // ColumnFileBig in DeltaValueSpace never pass a filter to DMFilePackFilter.
        // Assume its filter always return Some.
        std::transform(pack_res.cbegin(), pack_res.cend(), pack_res.begin(), [](RSResult a) {
            return a && RSResult::Some;
        });
    }

    auto [none_count, some_count, all_count, all_null_count] = countPackRes();
    auto after_filter = some_count + all_count + all_null_count;
    ProfileEvents::increment(ProfileEvents::DMFileFilterAftRoughSet, after_filter);
    // In table scanning, DMFilePackFilter of a DMFile may be created several times:
    // 1. When building MVCC bitmap (ReadTag::MVCC).
    // 2. When building LM filter stream (ReadTag::LM).
    // 3. When building stream of other columns (ReadTag::Query).
    // Only need to count the filter result once.
    // TODO: We can create DMFilePackFilter at the beginning and pass it to the stages described above.
    if (read_tag == ReadTag::Query)
    {
        scan_context->rs_pack_filter_none += none_count;
        scan_context->rs_pack_filter_some += some_count;
        scan_context->rs_pack_filter_all += all_count;
        scan_context->rs_pack_filter_all_null += all_null_count;
    }

    Float64 filter_rate = 0.0;
    if (after_read_packs != 0)
    {
        filter_rate = (after_read_packs - after_filter) * 100.0 / after_read_packs;
        GET_METRIC(tiflash_storage_rough_set_filter_rate, type_dtfile_pack).Observe(filter_rate);
    }
    LOG_DEBUG(
        log,
        "RSFilter exclude rate: {:.2f}, after_pk: {}, after_read_packs: {}, after_filter: {}, handle_ranges: {}"
        ", read_packs: {}, pack_count: {}, none_count: {}, some_count: {}, all_count: {}, all_null_count: {}, "
        "read_tag: {}",
        ((after_read_packs == 0) ? std::numeric_limits<double>::quiet_NaN() : filter_rate),
        after_pk,
        after_read_packs,
        after_filter,
        toDebugString(rowkey_ranges),
        ((read_packs == nullptr) ? 0 : read_packs->size()),
        pack_count,
        none_count,
        some_count,
        all_count,
        all_null_count,
        magic_enum::enum_name(read_tag));
}

std::tuple<UInt64, UInt64, UInt64, UInt64> DMFilePackFilter::countPackRes() const
{
    UInt64 none_count = 0;
    UInt64 some_count = 0;
    UInt64 all_count = 0;
    UInt64 all_null_count = 0;
    for (auto res : pack_res)
    {
        if (res == RSResult::None || res == RSResult::NoneNull)
            ++none_count;
        else if (res == RSResult::Some || res == RSResult::SomeNull)
            ++some_count;
        else if (res == RSResult::All)
            ++all_count;
        else if (res == RSResult::AllNull)
            ++all_null_count;
    }
    return {none_count, some_count, all_count, all_null_count};
}

UInt64 DMFilePackFilter::countUsePack() const
{
    return std::count_if(pack_res.cbegin(), pack_res.cend(), [](RSResult res) { return res.isUse(); });
}

class MinMaxIndexLoader
{
public:
    // Make the instance of `MinMaxIndexLoader` as a callable object that is used in
    // `index_cache->getOrSet(...)`.
    MinMaxIndexPtr operator()()
    {
        const auto index_file_size = dmfile->colIndexSize(col_id);
        if (index_file_size == 0)
            return std::make_shared<MinMaxIndex>(*col_type);

        const auto file_name_base = DMFile::getFileNameBase(col_id);
        auto index_guard = S3::S3RandomAccessFile::setReadFileInfo({
            .size = dmfile->getReadFileSize(col_id, colIndexFileName(file_name_base)),
            .scan_context = scan_context,
        });
        if (likely(dmfile->useMetaV2()))
        {
            // The min-max index is merged into ".merged" files, read from the file
            return loadMinMaxIndexFromMetav2(index_file_size, file_name_base);
        }
        else if (likely(dmfile->getConfiguration()))
        {
            // checksum is enabled but not merged into meta v2
            return loadMinMaxIndexWithChecksum(index_file_size, file_name_base);
        }
        else
        {
            // without checksum, simply load the raw bytes from file
            return loadRawMinMaxIndex(index_file_size, file_name_base);
        }
    }

public:
    MinMaxIndexLoader(
        const DMFilePtr & dmfile_,
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter_,
        const ColId col_id_,
        const DataTypePtr & col_type_,
        const ScanContextPtr & scan_context_)
        : dmfile(dmfile_)
        , file_provider(file_provider_)
        , read_limiter(read_limiter_)
        , col_id(col_id_)
        , col_type(col_type_)
        , scan_context(scan_context_)
    {}
    const DMFilePtr & dmfile;
    const FileProviderPtr & file_provider;
    const ReadLimiterPtr & read_limiter;
    const ColId col_id;
    const DataTypePtr & col_type;
    const ScanContextPtr & scan_context;

private:
    MinMaxIndexPtr loadRawMinMaxIndex(const size_t index_file_size, const FileNameBase & file_name_base) const
    {
        auto index_buf = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            dmfile->colIndexPath(file_name_base),
            dmfile->encryptionIndexPath(file_name_base),
            std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), index_file_size),
            read_limiter);
        return MinMaxIndex::read(*col_type, index_buf, index_file_size);
    }

    MinMaxIndexPtr loadMinMaxIndexWithChecksum(const size_t index_file_size, const FileNameBase & file_name_base) const
    {
        auto index_buf = ChecksumReadBufferBuilder::build(
            file_provider,
            dmfile->colIndexPath(file_name_base),
            dmfile->encryptionIndexPath(file_name_base),
            index_file_size,
            read_limiter,
            dmfile->getConfiguration()->getChecksumAlgorithm(),
            dmfile->getConfiguration()->getChecksumFrameLength());
        auto header_size = dmfile->getConfiguration()->getChecksumHeaderLength();
        auto frame_total_size = dmfile->getConfiguration()->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);
        return MinMaxIndex::read(*col_type, *index_buf, index_file_size - header_size * frame_count);
    }

    MinMaxIndexPtr loadMinMaxIndexFromMetav2(const size_t index_file_size, const FileNameBase & file_name_base) const
    {
        const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
        assert(dmfile_meta != nullptr);
        auto info = dmfile_meta->merged_sub_file_infos.find(colIndexFileName(file_name_base));
        RUNTIME_CHECK_MSG(
            info != dmfile_meta->merged_sub_file_infos.end(),
            "Unknown index file {}",
            dmfile->colIndexPath(file_name_base));

        auto file_path = dmfile->meta->mergedPath(info->second.number);
        auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
        auto offset = info->second.offset;
        auto data_size = info->second.size;

        const auto & config = dmfile->getConfiguration();

        auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
            file_provider,
            file_path,
            encryp_path,
            config->getChecksumFrameLength(),
            read_limiter);
        buffer.seek(offset);

        String raw_data;
        raw_data.resize(data_size);
        buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

        auto buf = ChecksumReadBufferBuilder::build(
            std::move(raw_data),
            dmfile->colIndexPath(file_name_base), // just for debug
            config->getChecksumFrameLength(),
            config->getChecksumAlgorithm(),
            config->getChecksumFrameLength());

        auto header_size = config->getChecksumHeaderLength();
        auto frame_total_size = config->getChecksumFrameLength() + header_size;
        auto frame_count = index_file_size / frame_total_size + (index_file_size % frame_total_size != 0);

        return MinMaxIndex::read(*col_type, *buf, index_file_size - header_size * frame_count);
    }
};

void DMFilePackFilter::loadIndex(
    ColumnIndexes & indexes,
    const DMFilePtr & dmfile,
    const FileProviderPtr & file_provider,
    const MinMaxIndexCachePtr & index_cache,
    bool set_cache_if_miss,
    ColId col_id,
    const ReadLimiterPtr & read_limiter,
    const ScanContextPtr & scan_context)
{
    const auto & type = dmfile->getColumnStat(col_id).type;
    const auto index_cache_key = dmfile->colIndexCacheKey(DMFile::getFileNameBase(col_id));

    MinMaxIndexPtr minmax_index;
    if (index_cache && set_cache_if_miss)
    {
        minmax_index = index_cache->getOrSet(
            index_cache_key,
            MinMaxIndexLoader(dmfile, file_provider, read_limiter, col_id, type, scan_context));
    }
    else
    {
        // try load from the cache first
        if (index_cache)
            minmax_index = index_cache->get(index_cache_key);
        // dose not hit the cache, load it
        if (minmax_index == nullptr)
            minmax_index = MinMaxIndexLoader(dmfile, file_provider, read_limiter, col_id, type, scan_context)();
    }
    indexes.emplace(col_id, RSIndex(type, minmax_index));
}

void DMFilePackFilter::tryLoadIndex(ColId col_id)
{
    if (param.indexes.count(col_id))
        return;

    if (!dmfile->isColIndexExist(col_id))
        return;

    Stopwatch watch;
    loadIndex(param.indexes, dmfile, file_provider, index_cache, set_cache_if_miss, col_id, read_limiter, scan_context);
}

} // namespace DB::DM
