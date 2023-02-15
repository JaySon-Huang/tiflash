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
#include <Common/Logger.h>
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
#include <aws/s3/model/ListObjectsV2Result.h>
#include <common/logger_useful.h>

#include <thread>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::S3
{
// FIXME: Remove this hardcode bucket
static constexpr auto BUCKET_NAME = "jayson";

S3GCManager::S3GCManager()
    : client(nullptr)
    , log(Logger::get())
{
    // TODO: support https
    const auto * access_key_id = "minioadmin";
    const auto * secret_access_key = "minioadmin";
    client = ClientFactory::instance().create(
        "172.16.5.85:9000",
        Aws::Http::Scheme::HTTP,
        false,
        access_key_id,
        secret_access_key);
}

void S3GCManager::runOnAllStores()
{
    const std::vector<UInt64> all_store_ids = getAllStoreIds();
    LOG_TRACE(log, "all_store_ids: {}", all_store_ids);
    for (const auto gc_store_id : all_store_ids)
    {
        runForStore(gc_store_id);
    }
}

void S3GCManager::runForStore(UInt64 gc_store_id)
{
    LOG_DEBUG(log, "run gc, gc_store_id={}", gc_store_id);
    // Get the latest manifest
    const ManifestListResult manifests = listManifest(gc_store_id);
    // clean the outdated manifest files
    removeOutdatedManifest(manifests);

    LOG_INFO(log, "latest manifest, gc_store_id={} upload_seq={} key={}", gc_store_id, manifests.latest_upload_seq, manifests.latest_manifest);
    // Parse from the latest manifest and collect valid lock files
    const std::unordered_set<String> valid_lock_files;
    // TODO: const std::unordered_set<String> valid_lock_files = getValidLocksFromManifest(manifests.latest_manifest);

    // Scan and remove the expired locks
    {
        // All locks share the same prefix
        const auto lock_prefix = S3Filename::getLockPrefix();
        cleanUnusedLocksOnPrefix(gc_store_id, lock_prefix, manifests.latest_upload_seq, valid_lock_files);
    }

    // After removing the expired lock, we need to scan the data files
    // with expired delmark
    tryCleanExpiredDataFiles(gc_store_id);
}

void S3GCManager::cleanUnusedLocksOnPrefix(
    UInt64 gc_store_id,
    String scan_prefix,
    UInt64 safe_sequence,
    const std::unordered_set<String> & valid_lock_files)
{
    // List the lock files under this prefix
    listPrefix(*client, BUCKET_NAME, scan_prefix, "", [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        for (const auto & object : objects)
        {
            const auto & lock_key = object.GetKey();
            LOG_TRACE(log, "lock_key={}", lock_key);
            const auto lock_filename_view = S3FilenameView::fromKey(lock_key);
            RUNTIME_CHECK(lock_filename_view.isLockFile(), lock_key);
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
            tryCleanLock(lock_key, lock_filename_view);
        }
        return objects.size();
    });
}

void S3GCManager::tryCleanLock(const String & lock_key, const S3FilenameView & lock_filename_view)
{
    const auto unlocked_datafilename_view = lock_filename_view.asDataFile();
    RUNTIME_CHECK(unlocked_datafilename_view.isDataFile());
    const auto unlocked_datafile_key = unlocked_datafilename_view.toFullKey();
    const auto unlocked_datafile_delmark_key = unlocked_datafilename_view.getDelMarkKey();

    // delete S3 lock file
    deleteObject(*client, BUCKET_NAME, lock_key);

    bool delmark_exists = false;
    Aws::Utils::DateTime mtime;
    std::tie(delmark_exists, mtime) = tryGetObjectModifiedTime(*client, BUCKET_NAME, unlocked_datafile_delmark_key);
    if (!delmark_exists)
    {
        // TODO: try create delmark through S3LockService
        uploadEmptyFile(*client, BUCKET_NAME, unlocked_datafile_delmark_key);
        LOG_INFO(log, "creating delmark, key={}", unlocked_datafile_key);
        return;
    }

    assert(delmark_exists); // function should return in previous if-branch
    removeDataFileIfDelmarkExpired(unlocked_datafile_key, unlocked_datafile_delmark_key, mtime);
}

void S3GCManager::removeDataFileIfDelmarkExpired(
    const String & datafile_key,
    const String & delmark_key,
    const Aws::Utils::DateTime & delmark_mtime)
{
    // delmark exist
    bool expired = false;
    {
        // Get the time diff by `now`-`mtime`
        Aws::Utils::DateTime now = Aws::Utils::DateTime::Now();
        auto diff_ms = Aws::Utils::DateTime::Diff(now, delmark_mtime).count();
        static constexpr Int64 DELMARK_EXPIRED_HOURS = 1;
        if (diff_ms > DELMARK_EXPIRED_HOURS * 3600 * 1000)
        {
            expired = true;
        }
        LOG_INFO(
            log,
            "delmark exist, datafile={} mark_time={} now={} diff_ms={} expired={}",
            datafile_key,
            delmark_mtime.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            now.ToGmtString(Aws::Utils::DateFormat::ISO_8601),
            diff_ms,
            expired);
    }
    // The delmark is not expired, wait for next GC round
    if (!expired)
        return;
    // The data file is marked as delete and delmark expired, safe to be
    // physical delete.
    deleteObject(*client, BUCKET_NAME, datafile_key); // TODO: it is safe to ignore if not exist
    LOG_INFO(log, "datafile deleted, key={}", datafile_key);
    // TODO: mock crash before deleting delmark on S3
    deleteObject(*client, BUCKET_NAME, delmark_key);
    LOG_INFO(log, "datafile delmark deleted, key={}", delmark_key);
}

void S3GCManager::tryCleanExpiredDataFiles(UInt64 gc_store_id)
{
    // StableFiles and CheckpointDataFile are stored with the same prefix, scan
    // the keys by prefix, and if there is an expired delmark, then try to remove
    // its correspond StableFile or CheckpointDataFile.
    const auto prefix = S3Filename::fromStoreId(gc_store_id).toDataPrefix();
    listPrefix(*client, BUCKET_NAME, prefix, "", [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        for (const auto & object : objects)
        {
            const auto & delmark_key = object.GetKey();
            LOG_TRACE(log, "key={}", object.GetKey());
            const auto filename_view = S3FilenameView::fromKey(delmark_key);
            // Only remove the data file with expired delmark
            if (!filename_view.isDelMark())
                continue;
            auto datafile_key = filename_view.asDataFile().toFullKey();
            removeDataFileIfDelmarkExpired(datafile_key, delmark_key, object.GetLastModified());
        }
        return objects.size();
    });
}

std::vector<UInt64> S3GCManager::getAllStoreIds() const
{
    std::vector<UInt64> all_store_ids;
    // The store key are "s${store_id}/", we need setting delimiter "/" to get the
    // common prefixes result.
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html
    listPrefix(*client, BUCKET_NAME, "s", "/", [&all_store_ids](const Aws::S3::Model::ListObjectsV2Result & result) {
        const Aws::Vector<Aws::S3::Model::CommonPrefix> & prefixes = result.GetCommonPrefixes();
        for (const auto & prefix : prefixes)
        {
            const auto filename_view = S3FilenameView::fromStoreKeyPrefix(prefix.GetPrefix());
            RUNTIME_CHECK(filename_view.type == S3FilenameType::StorePrefix, prefix.GetPrefix());
            all_store_ids.emplace_back(filename_view.store_id);
        }
        return prefixes.size();
    });

    return all_store_ids;
}

S3GCManager::ManifestListResult S3GCManager::listManifest(UInt64 store_id)
{
    Strings all_manifest;
    String latest_manifest;
    UInt64 latest_upload_seq = 0;

    const auto store_prefix = S3Filename::fromStoreId(store_id).toManifestPrefix();

    listPrefix(*client, BUCKET_NAME, store_prefix, "", [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        all_manifest.reserve(all_manifest.size() + objects.size());
        for (const auto & object : objects)
        {
            const auto & mf_key = object.GetKey();
            LOG_TRACE(log, "mf_key={}", mf_key);
            const auto filename_view = S3FilenameView::fromKey(mf_key);
            RUNTIME_CHECK(filename_view.type == S3FilenameType::CheckpointManifest, mf_key);
            // TODO: also store the object.GetLastModified() for removing
            // outdated manifest objects
            all_manifest.emplace_back(mf_key);
            auto upload_seq = filename_view.getUploadSequence();
            if (upload_seq > latest_upload_seq)
            {
                latest_upload_seq = upload_seq;
                latest_manifest = mf_key;
            }
        }
        return objects.size();
    });
    return ManifestListResult{
        .all_manifest = std::move(all_manifest),
        .latest_manifest = std::move(latest_manifest),
        .latest_upload_seq = latest_upload_seq,
    };
}

std::unordered_set<String> S3GCManager::getValidLocksFromManifest(const String & manifest_key)
{
    // TODO: download the latest manifest from S3 to local file
    const String local_manifest_path = getTemporaryDownloadFile(manifest_key);
    downloadFile(*client, BUCKET_NAME, local_manifest_path, manifest_key);
    LOG_INFO(log, "Download manifest, from={} to={}", manifest_key, local_manifest_path);
    using ManifestReader = PS::V3::CheckpointManifestFileReader<PS::V3::universal::PageDirectoryTrait>;
    auto reader = ManifestReader::create(ManifestReader::Options{.file_path = local_manifest_path});
    return reader->readLocks();
}

void S3GCManager::removeOutdatedManifest(const ManifestListResult & manifests)
{
    // TODO: clean the outdated manifest files
    UNUSED(this, manifests);
}

String S3GCManager::getTemporaryDownloadFile(String s3_key)
{
    UNUSED(this);
    std::replace(s3_key.begin(), s3_key.end(), '/', '_');
    return fmt::format("/tmp/{}_{}", s3_key, std::hash<std::thread::id>()(std::this_thread::get_id()));
}


} // namespace DB::S3
