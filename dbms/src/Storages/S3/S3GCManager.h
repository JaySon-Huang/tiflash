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

#include <Core/Types.h>
#include <common/types.h>

#include <memory>
#include <unordered_set>

namespace Aws
{
namespace S3
{
class S3Client;
} // namespace S3
namespace Utils
{
class DateTime;
} // namespace Utils
} // namespace Aws

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB

namespace DB::S3
{
struct S3FilenameView;

class S3GCManager
{
public:
    S3GCManager();

    void runOnAllStores();

private:
    void runForStore(UInt64 gc_store_id, const std::vector<UInt64> & all_store_ids);

    void cleanUnusedLocksOnPrefix(
        UInt64 gc_store_id,
        String scan_prefix,
        UInt64 safe_sequence,
        const std::unordered_set<String> & valid_lock_files);

    void tryCleanLock(const String & lock_key, const S3FilenameView & lock_filename_view);

    void tryCleanExpiredDataFiles(UInt64 gc_store_id);

    void removeDataFileIfDelmarkExpired(
        const String & datafile_key,
        const String & delmark_key,
        const Aws::Utils::DateTime & delmark_mtime);

    std::vector<UInt64> getAllStoreIds() const;

    struct ManifestListResult
    {
        Strings all_manifest;
        const String latest_manifest;
        const UInt64 latest_upload_seq;
    };

    ManifestListResult listManifest(UInt64 store_id);

    std::unordered_set<String> getValidLocksFromManifest(const String & manifest_key);

    void removeOutdatedManifest(const ManifestListResult & manifests);

    String getTemporaryDownloadFile(String s3_key);

private:
    std::shared_ptr<Aws::S3::S3Client> client;

    LoggerPtr log;
};
} // namespace DB::S3
