// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <benchmark/benchmark.h>
#include <common/logger_useful.h>

using DB::PS::V3::SpaceMap;

class SpacemapBM : public benchmark::Fixture
{
protected:
public:
    void SetUp(const ::benchmark::State & /*state*/) override
    {
    }
};

BENCHMARK_DEFINE_F(SpacemapBM, RBTree)
(benchmark::State & state)
{
    size_t max_cap = 512ULL * 1024 * 1024;
    auto space_map = SpaceMap::createSpaceMap(SpaceMap::SpaceMapType::SMAP64_RBTREE, 0, max_cap);
    for (auto _ : state)
    {
        for (size_t i = 0; i < (max_cap / 1024) - 1; ++i)
        {
            space_map->markUsed(i * 1024, 1024);
        }
        for (size_t i = 0; i < (max_cap / 1024); ++i)
        {
            space_map->markFree(i * 1024, 1024);
        }
    }
}
BENCHMARK_REGISTER_F(SpacemapBM, RBTree)->Iterations(200);

BENCHMARK_DEFINE_F(SpacemapBM, StdMap)
(benchmark::State & state)
{
    size_t max_cap = 512ULL * 1024 * 1024;
    auto space_map = SpaceMap::createSpaceMap(SpaceMap::SpaceMapType::SMAP64_STD_MAP, 0, max_cap);
    for (auto _ : state)
    {
        for (size_t i = 0; i < (max_cap / 1024) - 1; ++i)
        {
            space_map->markUsed(i * 1024, 1024);
        }
        for (size_t i = 0; i < (max_cap / 1024); ++i)
        {
            space_map->markFree(i * 1024, 1024);
        }
    }
}
BENCHMARK_REGISTER_F(SpacemapBM, StdMap)->Iterations(200);
