// Copyright 2025 PingCAP, Inc.
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
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{

bool parseTableID(const DecodedTiKVKey & key, TableID & table_id)
{
    auto k = key.getUserKey();
    // // t table_id _r
    // if (k.size() >= (1 + 8 + 2) && k[0] == RecordKVFormat::TABLE_PREFIX
    //     && memcmp(k.data() + 9, RecordKVFormat::RECORD_PREFIX_SEP, 2) == 0)
    // {
    //     table_id = RecordKVFormat::getTableId(k);
    //     return true;
    // }
    // t table_id
    if (k.size() >= (1 + 8) && k[0] == RecordKVFormat::TABLE_PREFIX)
    {
        table_id = RecordKVFormat::getTableId(k);
        return true;
    }

    return false;
}

RegionRangeKeys::RegionRangeKeys(TiKVKey && start_key, TiKVKey && end_key)
    : ori(RegionRangeKeys::makeComparableKeys(std::move(start_key), std::move(end_key)))
    , raw(std::make_shared<DecodedTiKVKey>(
              ori.first.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.first.key)),
          std::make_shared<DecodedTiKVKey>(
              ori.second.key.empty() ? DecodedTiKVKey() : RecordKVFormat::decodeTiKVKey(ori.second.key)))
{
    keyspace_id = raw.first->getKeyspaceID();
    bool parse_tid_ok = parseTableID(*raw.first, table_id);
    bool order_correct = ori.first.compare(ori.second) < 0;
    RUNTIME_CHECK_MSG(
        parse_tid_ok && order_correct,
        "Illegal region range, should not happen, start_key={} end_key={} parse_tid_ok={} order_correct={}",
        ori.first.key.toDebugString(),
        ori.second.key.toDebugString(),
        parse_tid_ok,
        order_correct);
}

RegionRangeKeys::RegionRange RegionRangeKeys::makeComparableKeys(TiKVKey && start_key, TiKVKey && end_key)
{
    return {
        TiKVRangeKey::makeTiKVRangeKey<true>(std::move(start_key)),
        TiKVRangeKey::makeTiKVRangeKey<false>(std::move(end_key)),
    };
}

RegionRangeKeys::RegionRange RegionRangeKeys::cloneRange(const RegionRange & from)
{
    return std::make_pair(from.first.copy(), from.second.copy());
}


} // namespace DB
