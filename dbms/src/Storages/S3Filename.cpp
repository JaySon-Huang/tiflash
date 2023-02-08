#include <Common/Exception.h>
#include <Storages/S3Filename.h>
#include <re2/re2.h>

#include <magic_enum.hpp>

namespace DB::S3
{
String S3FilenameView::getLockKey(UInt64 lock_store_id, UInt64 lock_seq) const
{
    return fmt::format("s{}/lock/{}.lock_s{}_{}", store_id, path, lock_store_id, lock_seq);
}

S3FilenameView parseFromS3Key(const String & fullpath)
{
    re2::RE2 rgx_data_file("^s([0-9]+)/(stable|data|lock)/(.+)(\\.lock_[0-9]+_[0-9]+)?$");
    S3FilenameView res{.type = S3FilenameType::Invalid};
    re2::StringPiece type_view, data_filepath, lock_suffix;
    if (re2::RE2::FullMatch(fullpath, rgx_data_file, &res.store_id, &type_view, &data_filepath, &lock_suffix))
    {
        if (type_view == "stable")
            res.type = S3FilenameType::StableFile;
        else if (type_view == "data")
            res.type = S3FilenameType::CheckpointDataFile;
        else if (type_view == "lock")
            res.type = S3FilenameType::LockFile;
        res.path = std::string_view(data_filepath.data(), data_filepath.size());
        res.lock_suffix = std::string_view(lock_suffix.data(), lock_suffix.size());
        return res;
    }
    return res;
}

S3Filename S3Filename::fromDMFileOID(const DM::Remote::DMFileOID & oid)
{
    return S3Filename{
        .type = S3FilenameType::StableFile,
        .store_id = oid.write_node_id,
        .path = fmt::format("t_{}/dmf_{}", oid.table_id, oid.file_id),
    };
}

String S3Filename::toFullKey() const
{
    switch (type)
    {
    case S3FilenameType::StableFile:
        return fmt::format("s{}/stable/{}", store_id, path);
    default:
        throw Exception(fmt::format("Not support type! type={}", magic_enum::enum_name(type)));
    }
}

} // namespace DB::S3
