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

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>

namespace DB::DM
{

class Equal : public ColCmpVal
{
public:
    Equal(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_)
    {}

    String name() override { return "equal"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        RSResults failure_results(pack_count, RSResult::Some);
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_DIRECTLY(param, attr, rsindex, failure_results);

        RSResults results(pack_count, RSResult::None);
        if (rsindex.minmax)
        {
            auto tmp_results = rsindex.minmax->checkCmp<RoughCheck::CheckEqual>(start_pack, pack_count, value, rsindex.type);
            for (size_t i = 0; i < pack_count; ++i)
            {
                results[i] = results[i] || tmp_results[i];
            }
        }
        if (rsindex.bloom_filter_index)
        {
            auto tmp_results = rsindex.bloom_filter_index
                                   ->checkEqual(start_pack, pack_count, value, rsindex.type);
            for (size_t i = 0; i < pack_count; ++i)
            {
                results[i] = results[i] || tmp_results[i];
            }
        }
       return results; 
    }
};

} // namespace DB::DM
