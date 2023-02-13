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

#pragma once

#include <Common/Logger.h>
#include <common/types.h>

#include <memory>
#include <unordered_set>

namespace Aws::S3
{
class S3Client;
}
namespace DB::S3
{
struct S3FilenameView;

class S3GCManager
{
public:
    void runOnAllStores();

private:
    void runForStore(UInt64 gc_store_id, const std::vector<UInt64> & all_store_ids);

    void cleanExpiredFilesOnPrefix(
        UInt64 gc_store_id,
        String scan_prefix,
        UInt64 safe_sequence,
        const std::unordered_set<String> & valid_lock_files);

    void tryCleanExpiredDataFile(const String & lock_key, const S3FilenameView & lock_filename_view);

private:
    std::shared_ptr<Aws::S3::S3Client> client;

    Logger log;
};
} // namespace DB::S3
