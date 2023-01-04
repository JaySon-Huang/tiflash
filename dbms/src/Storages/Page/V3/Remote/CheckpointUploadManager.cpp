
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointDataFileWriter.h>
#include <Storages/Page/V3/Remote/CheckpointFilesWriter.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileWriter.h>
#include <Storages/Page/V3/Remote/CheckpointUploadManager.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/MockS3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3EndpointProvider.h>
#include <aws/s3/model/PutObjectRequest.h>

namespace DB::PS::V3
{
CheckpointUploadManagerPtr CheckpointUploadManager::createForDebug(UInt64 store_id, PageDirectoryPtr & directory, BlobStorePtr & blob_store, DM::Remote::IDataStorePtr data_store)
{
    auto mgr = std::unique_ptr<CheckpointUploadManager>(new CheckpointUploadManager(directory, blob_store));
    mgr->store_id = store_id;
    mgr->data_store = data_store;

    return mgr;
}

CheckpointUploadManager::CheckpointUploadManager(PageDirectoryPtr & directory_, BlobStorePtr & blob_store_)
    : store_id(0)
    , page_directory(directory_)
    , blob_store(blob_store_)
    , log(Logger::get())
{
}

void CheckpointUploadManager::initStoreInfo(UInt64 actual_store_id)
{
    {
        std::unique_lock lock_init(mtx_store_init);
        // TODO: we need to restore the last_upload_sequence from S3
        store_id = actual_store_id;
    }
    cv_init.notify_all();
}

bool CheckpointUploadManager::createS3LockForWriteBatch(const UniversalWriteBatch & write_batch)
{
    {
        std::unique_lock lock_init(mtx_store_init);
        cv_init.wait(lock_init, [this]() { return this->store_id != 0; });
    }

    for (const auto & w : write_batch.getWrites())
    {
        if (w.type == WriteBatchWriteType::PUT_EXTERNAL)
        {
            if (w.remote)
            {
                auto oid = *data_store->parseFromFullpath(*w.remote->data_file_id);
                auto s3_fullpath = data_store->getDTFileRemoteFullPath(oid);
                createS3Lock(s3_fullpath, oid.write_node_id, store_id);
            }
        }
        else if (w.type == WriteBatchWriteType::PUT)
        {
            // apply a put that is actually stored in S3 instead of local
        }
    }
    return true;
}

bool CheckpointUploadManager::createS3Lock(std::string_view s3_file, UInt64 create_store_id, UInt64 lock_store_id)
{
    bool s3_lock_created = false;
    std::shared_lock manifest_lock(mtx_checkpoint_manifest);
    UInt64 upload_seq = last_upload_sequence + 1;
    String s3_lockfile_fullpath = fmt::format("{}.lock_s{}_{}", s3_file, lock_store_id, upload_seq); // TODO: Optimize for the lock path

    if (create_store_id == lock_store_id)
    {
        // Try to create a lock file for the data file uploaded by this store
        Aws::S3::Model::PutObjectRequest req;
        req.SetKey(s3_lockfile_fullpath);
        std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::StringStream>("");
        req.SetBody(input_data);

        auto outcome = [](Aws::S3::Model::PutObjectRequest & req) {
            Aws::InitAPI({});
            // Aws::Client::ClientConfiguration config;
            // config.region = Aws::Region::AWS_GLOBAL;
            // Aws::Auth::AWSCredentials credentials("minioadmin", "minioadmin");
            // auto endpoint = Aws::MakeShared<Aws::S3::S3EndpointProvider>("http://172.16.5.85:9000");
            String bucket_name = "jayson";

            req.SetBucket(bucket_name);

            // Aws::S3::S3Client s3_client(credentials, endpoint, config);
            // return s3_client.PutObject(req);
            MockS3Client mock_s3_client;
            return mock_s3_client.PutObject(req);
        }(req);
        LOG_DEBUG(log, "S3 lock created: {}", s3_lockfile_fullpath);
        // TODO: handle s3 network error. retry?
        RUNTIME_CHECK(outcome.IsSuccess(), outcome.GetError().GetMessage());
    }
    else
    {
        // TODO: Send rpc to S3LockService
        RUNTIME_CHECK(create_store_id == lock_store_id, create_store_id, lock_store_id);
    }

    if (!s3_lock_created)
        return false;

    pre_locks_files.emplace(s3_lockfile_fullpath);
    return s3_lock_created;
}

void CheckpointUploadManager::cleanAppliedS3ExternalFiles(std::set<String> && applied_s3files)
{
    std::shared_lock manifest_lock(mtx_checkpoint_manifest);
    for (const auto & file : applied_s3files)
    {
        pre_locks_files.erase(file);
    }
}

CheckpointUploadManager::DumpRemoteCheckpointResult
CheckpointUploadManager::dumpRemoteCheckpoint(DumpRemoteCheckpointOptions options)
{
    using Trait = PS::V3::universal::PageDirectoryTrait;
    std::scoped_lock lock(mtx_checkpoint);

    RUNTIME_CHECK(endsWith(options.temp_directory, "/"));
    RUNTIME_CHECK(endsWith(options.remote_directory, "/"));
    RUNTIME_CHECK(!options.data_file_name_pattern.empty());
    RUNTIME_CHECK(!options.manifest_file_name_pattern.empty());

    // FIXME: We need to dump snapshot from files, in order to get a correct `being_ref_count`.
    //  Note that, snapshots from files does not have a correct remote info, so we cannot simply
    //  copy logic from `tryDumpSnapshot`.
    //  Currently this is fine, because we will not reclaim data from the PageStorage.

    LOG_INFO(log, "Start dumpRemoteCheckpoint");

    // Let's keep this snapshot until all finished, so that blob data will not be GCed.
    auto snap = page_directory->createSnapshot(/*tracing_id*/ "");

    if (snap->sequence == last_checkpoint_sequence)
    {
        LOG_INFO(log, "Skipped dump checkpoint because sequence is unchanged, last_seq={} this_seq={}", last_checkpoint_sequence, snap->sequence);
        return {};
    }

    auto edit_from_mem = page_directory->dumpSnapshotToEdit(snap);
    LOG_DEBUG(log, "Dumped edit from PageDirectory, snap_seq={} n_edits={}", snap->sequence, edit_from_mem.size());

    // As a checkpoint, we write both entries (in manifest) and its data.
    // Some entries' data may be already written by a previous checkpoint. These data will not be written again.

    // TODO: Check temp file exists.

    auto data_file_name = fmt::format(
        options.data_file_name_pattern,
        fmt::arg("sequence", snap->sequence),
        fmt::arg("sub_file_index", 0));
    auto remote_data_file_path = options.remote_directory + data_file_name;
    auto remote_data_file_path_tmp = remote_data_file_path + ".tmp";
    // Always append a suffix, in case of remote_directory == temp_directory
    auto local_data_file_path_temp = options.temp_directory + data_file_name + ".tmp";

    auto manifest_file_name = fmt::format(
        options.manifest_file_name_pattern,
        fmt::arg("sequence", snap->sequence));
    auto remote_manifest_file_path = options.remote_directory + manifest_file_name;
    auto remote_manifest_file_path_temp = remote_manifest_file_path + ".tmp";
    // Always append a suffix, in case of remote_directory == temp_directory
    auto local_manifest_file_path_temp = options.temp_directory + manifest_file_name + ".tmp";

    Poco::File(Poco::Path(local_data_file_path_temp).parent()).createDirectories();
    Poco::File(Poco::Path(local_manifest_file_path_temp).parent()).createDirectories();

    LOG_DEBUG(log, "data_file_path_temp={} manifest_file_path_temp={}", local_data_file_path_temp, local_manifest_file_path_temp);

    std::unique_lock manifest_lock(mtx_checkpoint_manifest); // TODO: this lock can be acquire after all data dumped but before wrting into manifest
    UInt64 current_upload_sequence = last_upload_sequence + 1;

    auto data_writer = CheckpointDataFileWriter<Trait>::create(
        typename CheckpointDataFileWriter<Trait>::Options{
            .file_path = local_data_file_path_temp,
            .file_id = data_file_name,
        });
    auto manifest_writer = CheckpointManifestFileWriter<Trait>::create(
        typename CheckpointManifestFileWriter<Trait>::Options{
            .file_path = local_manifest_file_path_temp,
            .file_id = manifest_file_name,
        });
    auto writer = CheckpointFilesWriter<Trait>::create(
        typename CheckpointFilesWriter<Trait>::Options{
            .info = typename CheckpointFilesWriter<Trait>::Info{
                .writer = options.writer_info,
                .sequence = snap->sequence,
                .last_sequence = 0,
            },
            .data_writer = std::move(data_writer),
            .manifest_writer = std::move(manifest_writer),
            .blob_store = blob_store,
            .log = log,
        });

    writer->writePrefix();
    bool has_new_data = writer->writeEditsAndApplyRemoteInfo(edit_from_mem);
    writer->writeSuffix();

    writer.reset();

    if (has_new_data)
    {
        // Copy back the remote info to the current PageStorage. New remote infos are attached in `writeEditsAndApplyRemoteInfo`.
        // Snapshot cannot prevent obsolete entries from being deleted.
        // For example, if there is a `Put 1` with sequence 10, `Del 1` with sequence 11,
        // and the snapshot sequence is 12, Page with id 1 may be deleted by the gc process.
        page_directory->copyRemoteInfoFromEdit(edit_from_mem, /* allow_missing */ true);
    }

    // NOTE: The following IO may be very slow, because the output directory should be mounted as S3.
    Poco::File(Poco::Path(remote_data_file_path).parent()).createDirectories();
    Poco::File(Poco::Path(remote_manifest_file_path).parent()).createDirectories();

    auto data_file = Poco::File{local_data_file_path_temp};
    RUNTIME_CHECK(data_file.exists());

    if (has_new_data)
    {
        // Upload in two steps to avoid other store read incomplete file
        if (remote_data_file_path_tmp != local_data_file_path_temp)
        {
            data_file.moveTo(remote_data_file_path_tmp);
        }
        auto remote_data_file_temp = Poco::File{remote_data_file_path_tmp};
        RUNTIME_CHECK(remote_data_file_temp.exists());
        remote_data_file_temp.renameTo(remote_data_file_path);
    }
    else
        data_file.remove();

    auto manifest_file = Poco::File{local_manifest_file_path_temp};
    RUNTIME_CHECK(manifest_file.exists());
    if (remote_manifest_file_path_temp != local_manifest_file_path_temp)
    {
        manifest_file.moveTo(remote_manifest_file_path_temp);
    }
    auto remote_manifest_file_temp = Poco::File{remote_manifest_file_path_temp};
    RUNTIME_CHECK(remote_manifest_file_temp.exists());
    remote_manifest_file_temp.renameTo(remote_manifest_file_path);

    last_upload_sequence = current_upload_sequence;
    last_checkpoint_sequence = snap->sequence;
    LOG_DEBUG(log, "Update last_checkpoint_sequence to {}", last_checkpoint_sequence);

    return DumpRemoteCheckpointResult{
        .data_file = data_file, // Note: when has_new_data == false, this field will be pointing to a file not exist. To be fixed.
        .manifest_file = manifest_file,
    };
}


} // namespace DB::PS::V3
