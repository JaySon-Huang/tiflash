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

#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/Page/V3/spacemap/SpaceMapSTDMap.h>
#include <benchmark/benchmark.h>
#include <common/logger_useful.h>

#include <random>

namespace DB::bench
{

using namespace PS::V3;

static void SpaceMapBM(benchmark::State & state)
{
    std::random_device rd; // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()

    auto space_map = SpaceMap::createSpaceMap(SpaceMap::SpaceMapType::SMAP64_STD_MAP, 0, 128 * 1024 * 1024);

    size_t avg_size = 1 * 1024;
    std::uniform_int_distribution<> distrib(1, avg_size * 2);
    for (auto _ : state)
    {
        auto rand_size = distrib(gen);
        auto [offset, max_cap, _ignore] = space_map->searchInsertOffset(rand_size);
        space_map->markUsed(offset, rand_size);
    }
}
BENCHMARK(SpaceMapBM);


} // namespace DB::bench
