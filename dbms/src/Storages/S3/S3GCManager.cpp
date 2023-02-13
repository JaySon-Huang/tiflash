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

#include <Common/Exception.h>
#include <Core/Types.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3GCManager.h>
#include <TestUtils/MockS3Client.h>
#include <aws/core/utils/DateTime.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CommonPrefix.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::S3
{
// FIXME: Remove this hardcode bucket
static constexpr auto BUCKET_NAME = "jayson";

std::vector<UInt64> getAllStoresFromS3(const std::shared_ptr<Aws::S3::S3Client> & s3_client)
{
    Aws::S3::Model::ListObjectsV2Request req;
    req.SetBucket(BUCKET_NAME);
    req.WithPrefix("s").WithDelimiter("/");
    auto outcome = s3_client->ListObjectsV2(req);
    RUNTIME_CHECK(outcome.IsSuccess(), outcome.GetError().GetMessage());

    // TODO: handle the result size over max size
    const auto & result = outcome.GetResult();
    RUNTIME_CHECK(result.GetIsTruncated(), result.GetIsTruncated(), result.GetNextContinuationToken());

    Aws::Vector<Aws::S3::Model::CommonPrefix> prefixes = result.GetCommonPrefixes();

    std::vector<UInt64> all_store_ids;
    for (const auto & prefix : prefixes)
    {
        auto filename_view = S3FilenameView::fromStoreKeyPrefix(prefix.GetPrefix());
        RUNTIME_CHECK(filename_view.type == S3FilenameType::StorePrefix, prefix.GetPrefix());
        all_store_ids.emplace_back(filename_view.store_id);
    }

    return all_store_ids;
}

void S3GCManager::runOnAllStores()
{
    std::vector<UInt64> all_store_ids = getAllStoresFromS3(client);
    for (const auto gc_store_id : all_store_ids)
    {
        runForStore(gc_store_id, all_store_ids);
    }
}

void S3GCManager::runForStore(UInt64 gc_store_id, const std::vector<UInt64> & all_store_ids)
{
    // Get the latest manifest
    Strings all_manifest;
    String latest_manifest;
    UInt64 latest_upload_seq = 0;
    {
        auto store_prefix = S3Filename::fromStoreId(gc_store_id).toFullKey();
        all_manifest = listPrefix(*client, BUCKET_NAME, store_prefix);
        for (const auto & mf_key : all_manifest)
        {
            auto filename_view = S3FilenameView::fromKey(mf_key);
            RUNTIME_CHECK(filename_view.type == S3FilenameType::CheckpointManifest);
            auto upload_seq = filename_view.getUploadSequence();
            if (upload_seq > latest_upload_seq)
            {
                latest_upload_seq = upload_seq;
                latest_manifest = mf_key;
            }
        }
        // TODO: clean the outdated manifest files
    }

    // Parse from the latest manifest and collect valid lock files
    std::unordered_set<String> valid_lock_files;
    {
        String local_manifest_path;
        using ManifestReader = PS::V3::CheckpointManifestFileReader<PS::V3::universal::PageDirectoryTrait>;
        auto reader = ManifestReader::create(ManifestReader::Options{.file_path = local_manifest_path});
        valid_lock_files = reader->readLocks();
    }

    for (const auto & store_id : all_store_ids)
    {
        auto scan_prefix = fmt::format("s{}/lock/", store_id);
        cleanExpiredFilesOnPrefix(gc_store_id, scan_prefix, latest_upload_seq, valid_lock_files);
    }
}

void S3GCManager::cleanExpiredFilesOnPrefix(
    UInt64 gc_store_id,
    String scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files)
{
    // List the lock files under this prefix
    Strings lock_keys = listPrefix(*client, BUCKET_NAME, scan_prefix);
    for (const auto & lock_key : lock_keys)
    {
        const auto lock_filename_view = S3FilenameView::fromKey(lock_key);
        RUNTIME_CHECK(lock_filename_view.isLockFile());
        const auto lock_info = lock_filename_view.getLockInfo();
        // The lock file is not managed by `gc_store_id`, skip
        if (lock_info.store_id != gc_store_id)
            continue;
        // The lock is not managed by the latest manifest yet, wait for
        // next GC round
        if (lock_info.sequence > safe_sequence)
            continue;
        // The lock is still valid
        if (valid_lock_files.count(lock_key) > 0)
            continue;

        // The data file is not used by `gc_store_id` anymore, remove the lock file
        tryCleanExpiredDataFile(lock_key, lock_filename_view);
    }
}

void S3GCManager::tryCleanExpiredDataFile(const String & lock_key, const S3FilenameView & lock_filename_view)
{
    const auto unlocked_datafilename_view = lock_filename_view.asDataFile();
    RUNTIME_CHECK(unlocked_datafilename_view.isDataFile());
    const auto unlocked_datafile_delmark_key = unlocked_datafilename_view.getDelMarkKey();

    // delete S3 lock file
    deleteObject(*client, BUCKET_NAME, lock_key);

    bool delmark_exists = false;
    Aws::Utils::DateTime mtime;
    std::tie(delmark_exists, mtime) = tryGetObjectModifiedTime(*client, BUCKET_NAME, unlocked_datafile_delmark_key);
    if (!delmark_exists)
    {
        // TODO: try create delmark through S3LockService
        return;
    }

    assert(delmark_exists);
    // delmark exist
    bool expired = false;
    // The delmark is not expired, wait for next GC round
    if (!expired)
        return;
    // The data file is marked as delete and delmark expired, safe to be
    // physical delete.
    const auto unlocked_datafile_key = unlocked_datafilename_view.toFullKey();
    deleteObject(*client, BUCKET_NAME, unlocked_datafile_key);
    deleteObject(*client, BUCKET_NAME, unlocked_datafile_delmark_key);
}

} // namespace DB::S3
