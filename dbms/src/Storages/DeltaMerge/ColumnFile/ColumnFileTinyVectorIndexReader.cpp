// Copyright 2024 PingCAP, Inc.
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

#include <Common/EventRecorder.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyVectorIndexReader.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>


namespace DB::DM
{

void ColumnFileTinyVectorIndexReader::read(
    MutableColumnPtr & vec_column,
    const std::span<const VectorIndexReader::Key> & read_rowids,
    size_t rowid_start_offset)
{
    RUNTIME_CHECK(loaded);

    Stopwatch watch;
    vec_column->reserve(read_rowids.size());
    std::vector<Float32> value;
    for (const auto & rowid : read_rowids)
    {
        // Each ColomnFileTiny has its own vector index, rowid_start_offset is the offset of the ColmnFilePersistSet.
        vec_index->get(rowid - rowid_start_offset, value);
        vec_column->insertData(reinterpret_cast<const char *>(value.data()), value.size() * sizeof(Float32));
    }

    perf_stat.returned_rows = read_rowids.size();
    perf_stat.read_vec_column_seconds = watch.elapsedSeconds();
}

std::vector<VectorIndexReader::SearchResult> ColumnFileTinyVectorIndexReader::load()
{
    if (loaded)
        return {};

    Stopwatch watch;

    loadVectorIndex();
    auto search_results = loadVectorSearchResult();

    perf_stat.load_vec_index_and_results_seconds = watch.elapsedSeconds();

    loaded = true;
    return search_results;
}

void ColumnFileTinyVectorIndexReader::loadVectorIndex()
{
    const auto & index_infos = tiny_file.index_infos;
    if (!index_infos || index_infos->empty())
        return;
    auto index_id = ann_query_info->index_id();
    const auto index_info_iter
        = std::find_if(index_infos->cbegin(), index_infos->cend(), [index_id](const auto & info) {
              return info.index_props().index_id() == index_id;
          });
    if (index_info_iter == index_infos->cend())
        return;
    if (!index_info_iter->index_props().has_vector_index())
        return;
    auto index_page_id = index_info_iter->index_page_id();
    auto load_from_page_storage = [&]() {
        perf_stat.load_from_cache = false;
        std::vector<size_t> index_fields = {0};
        auto index_page = data_provider->readTinyData(index_page_id, index_fields);
        ReadBufferFromOwnString read_buf(index_page.data);
        CompressedReadBuffer compressed(read_buf);
        return VectorIndexReader::createFromMemory(index_info_iter->index_props().vector_index(), compressed);
    };
    if (local_index_cache)
    {
        const auto key = fmt::format("{}{}", LocalIndexCache::COLUMNFILETINY_INDEX_NAME_PREFIX, index_page_id);
        auto local_index = local_index_cache->getOrSet(key, load_from_page_storage);
        vec_index = std::dynamic_pointer_cast<VectorIndexReader>(local_index);
    }
    else
        vec_index = load_from_page_storage();
}

ColumnFileTinyVectorIndexReader::~ColumnFileTinyVectorIndexReader()
{
    LOG_DEBUG(
        log,
        "Finish vector search over column tiny_{}/{}(cid={}, rows={}){} cached, cost_[search/read]={:.3f}s/{:.3f}s "
        "top_k_[query/visited/discarded/result]={}/{}/{}/{} ",
        tiny_file.getDataPageId(),
        vec_cd.name,
        vec_cd.id,
        tiny_file.getRows(),
        perf_stat.load_from_cache ? "" : " not",

        perf_stat.load_vec_index_and_results_seconds,
        perf_stat.read_vec_column_seconds,

        ann_query_info->top_k(),
        perf_stat.visited_nodes, // Visited nodes will be larger than query_top_k when there are MVCC rows
        perf_stat.discarded_nodes, // How many nodes are skipped by MVCC
        perf_stat.returned_rows);
}

std::vector<VectorIndexReader::SearchResult> ColumnFileTinyVectorIndexReader::loadVectorSearchResult()
{
    auto perf_begin = PerfContext::vector_search;
    RUNTIME_CHECK(valid_rows.size() == tiny_file.getRows(), valid_rows.size(), tiny_file.getRows());

    auto search_results = vec_index->search(ann_query_info, valid_rows);
    // Sort by key
    std::sort(search_results.begin(), search_results.end(), [](const auto & lhs, const auto & rhs) {
        return lhs.key < rhs.key;
    });
    // results must not contain duplicates. Usually there should be no duplicates.
    search_results.erase(
        std::unique(
            search_results.begin(),
            search_results.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.key == rhs.key; }),
        search_results.end());

    perf_stat.discarded_nodes = PerfContext::vector_search.discarded_nodes - perf_begin.discarded_nodes;
    perf_stat.visited_nodes = PerfContext::vector_search.visited_nodes - perf_begin.visited_nodes;
    return search_results;
}

} // namespace DB::DM
