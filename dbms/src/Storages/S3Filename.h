#pragma once

#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <common/types.h>
#include <fmt/format.h>

#include <string_view>

namespace DB::S3
{
enum class S3FilenameType
{
    Invalid,
    StableFile,
    CheckpointDataFile,
    CheckpointManifest,
    LockFile,
};

struct S3FilenameView
{
    S3FilenameType type{S3FilenameType::Invalid};
    UInt64 store_id{0};
    std::string_view path;
    std::string_view lock_suffix;

    String getLockKey(UInt64 lock_store_id, UInt64 lock_seq) const;

    bool isDataFile() const { return type == S3FilenameType::StableFile || type == S3FilenameType::CheckpointDataFile; }
};

struct S3Filename
{
    S3FilenameType type{S3FilenameType::Invalid};
    UInt64 store_id{0};
    String path;

    static S3Filename fromDMFileOID(const DM::Remote::DMFileOID & oid);
    static S3Filename newCheckpointData(UInt64 store_id, UInt64 upload_seq, UInt64 file_idx);
    static S3Filename newCheckpointManifest(UInt64 store_id, UInt64 upload_seq);

    String toFullKey() const;

    S3FilenameView toView() const
    {
        return S3FilenameView{
            .type = type,
            .store_id = store_id,
            .path = path,
        };
    }
};

S3FilenameView parseFromS3Key(const String & fullpath);

} // namespace DB::S3
