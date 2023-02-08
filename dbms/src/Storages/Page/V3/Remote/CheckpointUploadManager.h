#include <Common/nocopyable.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/Proto/common.pb.h>
#include <Storages/S3Filename.h>
#include <common/types.h>

#include <condition_variable>

namespace DB
{
class Context;
}

namespace DB::PS::V3
{
class CheckpointUploadManager;
using CheckpointUploadManagerPtr = std::unique_ptr<CheckpointUploadManager>;


// TODO: Move it into Page/universal
class CheckpointUploadManager
{
public:
    using PageDirectoryPtr = PS::V3::universal::PageDirectoryPtr;
    using BlobStorePtr = PS::V3::universal::BlobStorePtr;

    static CheckpointUploadManagerPtr createForDebug(UInt64 store_id, PageDirectoryPtr & directory, BlobStorePtr & blob_store);

    void initStoreInfo(UInt64 store_id);

    bool createS3LockForWriteBatch(UniversalWriteBatch & write_batch);

    struct S3LockCreateResult
    {
        String lock_key;
        String err_msg;
        bool ok() const { return !lock_key.empty(); }
    };
    S3LockCreateResult createS3Lock(const S3::S3FilenameView & s3_file, UInt64 lock_store_id);

    void cleanAppliedS3ExternalFiles(std::unordered_set<String> && applied_s3files);

#if 1
    struct DumpRemoteCheckpointOptions
    {
        /**
         * The directory where temporary files are generated.
         * Files are first generated in the temporary directory, then copied into the remote directory.
         */
        const std::string & temp_directory;

        /**
         * The writer info field in the dumped files.
         */
        const std::shared_ptr<const Remote::WriterInfo> writer_info;

        const ReadLimiterPtr read_limiter = nullptr;
        const WriteLimiterPtr write_limiter = nullptr;
    };

    struct DumpRemoteCheckpointResult
    {
        Strings data_file;
        String manifest_file;
    };

    DumpRemoteCheckpointResult dumpRemoteCheckpoint(DumpRemoteCheckpointOptions options);
#else
    struct DumpRemoteCheckpointOptions
    {
        /**
         * The directory where temporary files are generated.
         * Files are first generated in the temporary directory, then copied into the remote directory.
         */
        const std::string & temp_directory;

        /**
         * Final files are always named according to `data_file_name_pattern` and `manifest_file_name_pattern`.
         * When we support different remote endpoints, the definition of remote_directory will change.
         */
        const std::string & remote_directory;

        /**
         * The data file name. Available placeholders: {sequence}, {sub_file_index}.
         * We accept "/" in the file name.
         */
        const std::string & data_file_name_pattern;

        /**
         * The manifest file name. Available placeholders: {sequence}.
         * We accept "/" in the file name.
         */
        const std::string & manifest_file_name_pattern;

        /**
         * The writer info field in the dumped files.
         */
        const std::shared_ptr<const Remote::WriterInfo> writer_info;

        const ReadLimiterPtr read_limiter = nullptr;
        const WriteLimiterPtr write_limiter = nullptr;
    };

    struct DumpRemoteCheckpointResult
    {
        Poco::File data_file;
        Poco::File manifest_file;
    };

    DumpRemoteCheckpointResult dumpRemoteCheckpoint(DumpRemoteCheckpointOptions options);
#endif


    DISALLOW_COPY(CheckpointUploadManager);

private:
    explicit CheckpointUploadManager(PageDirectoryPtr & directory_, BlobStorePtr & blob_store_);

private:
    UInt64 store_id;

    PageDirectoryPtr & page_directory;
    BlobStorePtr & blob_store;

    std::mutex mtx_store_init;
    std::condition_variable cv_init;


    std::mutex mtx_checkpoint;
    UInt64 last_checkpoint_sequence = 0;

    std::shared_mutex mtx_checkpoint_manifest;
    std::atomic<UInt64> last_upload_sequence = 0;
    std::unordered_set<String> pre_locks_files;

    LoggerPtr log;
};

} // namespace DB::PS::V3
