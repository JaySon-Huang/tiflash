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
#include <Storages/S3/S3Filename.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <magic_enum.hpp>
#include <string_view>

namespace DB::S3
{

//==== Serialize/Deserialize ====//

namespace details
{
String toFullKey(const S3FilenameType type, const UInt64 store_id, const std::string_view path)
{
    switch (type)
    {
    case S3FilenameType::StableFile:
        return fmt::format("s{}/data/{}", store_id, path);
    case S3FilenameType::CheckpointDataFile:
        return fmt::format("s{}/data/{}", store_id, path);
    case S3FilenameType::CheckpointManifest:
        return fmt::format("s{}/manifest/{}", store_id, path);
    case S3FilenameType::StorePrefix:
        return fmt::format("s{}/", store_id);
    default:
        throw Exception(fmt::format("Not support type! type={}", magic_enum::enum_name(type)));
    }
    __builtin_unreachable();
}

static constexpr std::string_view DELMARK_SUFFIX = ".del";
} // namespace details

String S3FilenameView::toFullKey() const
{
    return details::toFullKey(type, store_id, path);
}

String S3Filename::toFullKey() const
{
    return details::toFullKey(type, store_id, path);
}

String S3Filename::toManifestPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return details::toFullKey(type, store_id, path) + "manifest/";
}

String S3Filename::toDataPrefix() const
{
    RUNTIME_CHECK(type == S3FilenameType::StorePrefix);
    return details::toFullKey(type, store_id, path) + "data/";
}

S3FilenameView S3FilenameView::fromKey(const std::string_view fullpath)
{
    const static re2::RE2 rgx_data_file("^s([0-9]+)/(data|lock|manifest)/(.+)$");
    S3FilenameView res{.type = S3FilenameType::Invalid};
    re2::StringPiece fullpath_sp{fullpath.data(), fullpath.size()};
    re2::StringPiece type_view, data_filepath;
    if (!re2::RE2::FullMatch(fullpath_sp, rgx_data_file, &res.store_id, &type_view, &data_filepath))
        return res;

    if (type_view == "manifest")
        res.type = S3FilenameType::CheckpointManifest;
    else if (type_view == "data")
    {
        bool is_delmark = data_filepath.ends_with(re2::StringPiece(details::DELMARK_SUFFIX.data(), details::DELMARK_SUFFIX.size()));
        if (data_filepath.starts_with("dat_"))
        {
            // "dat_${upload_seq}_${idx}"
            if (is_delmark)
            {
                data_filepath.remove_suffix(details::DELMARK_SUFFIX.size());
                res.type = S3FilenameType::DelMarkToCheckpointData;
            }
            else
            {
                res.type = S3FilenameType::CheckpointDataFile;
            }
        }
        else if (data_filepath.starts_with("t_"))
        {
            // "t_${table_id}/dmf_${id}"
            if (is_delmark)
            {
                data_filepath.remove_suffix(details::DELMARK_SUFFIX.size());
                res.type = S3FilenameType::DelMarkToStableFile;
            }
            else
            {
                res.type = S3FilenameType::StableFile;
            }
        }
        else
        {
            res.type = S3FilenameType::Invalid;
        }
    }
    else if (type_view == "lock")
    {
        const auto lock_start_npos = data_filepath.find(".lock_");
        if (lock_start_npos == re2::StringPiece::npos)
        {
            res.type = S3FilenameType::Invalid;
            return res;
        }
        if (data_filepath.starts_with("t_"))
            res.type = S3FilenameType::LockFileToStableFile;
        else if (data_filepath.starts_with("dat_"))
            res.type = S3FilenameType::LockFileToCheckpointData;
        else
        {
            res.type = S3FilenameType::Invalid;
            return res;
        }
        res.lock_suffix = std::string_view(data_filepath.begin() + lock_start_npos, data_filepath.size() - lock_start_npos);
        data_filepath.remove_suffix(res.lock_suffix.size());
    }
    res.path = std::string_view(data_filepath.data(), data_filepath.size());
    return res;
}

S3FilenameView S3FilenameView::fromStoreKeyPrefix(const std::string_view prefix)
{
    const static re2::RE2 rgx_pattern("^s([0-9]+)/$");
    S3FilenameView res{.type = S3FilenameType::Invalid};
    re2::StringPiece prefix_sp{prefix.data(), prefix.size()};
    if (!re2::RE2::FullMatch(prefix_sp, rgx_pattern, &res.store_id))
        return res;

    res.type = S3FilenameType::StorePrefix;
    return res;
}

//==== Data file utils ====//

String S3FilenameView::getLockKey(UInt64 lock_store_id, UInt64 lock_seq) const
{
    RUNTIME_CHECK(isDataFile());
    return fmt::format("s{}/lock/{}.lock_s{}_{}", store_id, path, lock_store_id, lock_seq);
}

String S3FilenameView::getDelMarkKey() const
{
    switch (type)
    {
    case S3FilenameType::StableFile:
        return fmt::format("s{}/data/{}{}", store_id, path, details::DELMARK_SUFFIX);
    case S3FilenameType::CheckpointDataFile:
        return fmt::format("s{}/data/{}{}", store_id, path, details::DELMARK_SUFFIX);
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

UInt64 S3FilenameView::getUploadSequence() const
{
    UInt64 upload_seq = 0;
    UInt64 file_idx = 0;
    switch (type)
    {
    case S3FilenameType::CheckpointManifest:
    {
        re2::StringPiece path_sp{path.data(), path.size()};
        if (!re2::RE2::FullMatch(path_sp, "mf_([0-9]+)", &upload_seq))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid {}, path={}", magic_enum::enum_name(type), path);
        return upload_seq;
    }
    case S3FilenameType::CheckpointDataFile:
    {
        re2::StringPiece path_sp{path.data(), path.size()};
        if (!re2::RE2::FullMatch(path_sp, "dat_([0-9]+)_([0-9]+)", &upload_seq, &file_idx))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid {}, path={}", magic_enum::enum_name(type), path);
        return upload_seq;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

//==== Lock file utils ====//

S3FilenameView S3FilenameView::asDataFile() const
{
    switch (type)
    {
    case S3FilenameType::LockFileToStableFile:
        return S3FilenameView{.type = S3FilenameType::StableFile, .store_id = store_id, .path = path};
    case S3FilenameType::LockFileToCheckpointData:
        return S3FilenameView{.type = S3FilenameType::CheckpointDataFile, .store_id = store_id, .path = path};
    case S3FilenameType::DelMarkToStableFile:
        return S3FilenameView{.type = S3FilenameType::StableFile, .store_id = store_id, .path = path};
    case S3FilenameType::DelMarkToCheckpointData:
        return S3FilenameView{.type = S3FilenameType::CheckpointDataFile, .store_id = store_id, .path = path};
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

S3FilenameView::LockInfo S3FilenameView::getLockInfo() const
{
    LockInfo lock_info;
    switch (type)
    {
    case S3FilenameType::LockFileToCheckpointData:
    case S3FilenameType::LockFileToStableFile:
    {
        RUNTIME_CHECK(!lock_suffix.empty());
        re2::StringPiece lock_suffix_sp{lock_suffix.data(), lock_suffix.size()};
        if (!re2::RE2::FullMatch(lock_suffix_sp, ".lock_s([0-9]+)_([0-9]+)", &lock_info.store_id, &lock_info.sequence))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid {}, lock_suffix={}", magic_enum::enum_name(type), lock_suffix);
        return lock_info;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupport type: {}", magic_enum::enum_name(type));
    }
    __builtin_unreachable();
}

//==== Generate S3 key from raw parts ====//

S3Filename S3Filename::fromStoreId(UInt64 store_id)
{
    return S3Filename{
        .type = S3FilenameType::StorePrefix,
        .store_id = store_id,
    };
}

S3Filename S3Filename::fromDMFileOID(const DM::Remote::DMFileOID & oid)
{
    return S3Filename{
        .type = S3FilenameType::StableFile,
        .store_id = oid.write_node_id,
        .path = fmt::format("t_{}/dmf_{}", oid.table_id, oid.file_id),
    };
}

S3Filename S3Filename::newCheckpointData(UInt64 store_id, UInt64 upload_seq, UInt64 file_idx)
{
    return S3Filename{
        .type = S3FilenameType::CheckpointDataFile,
        .store_id = store_id,
        .path = fmt::format("dat_{}_{}", upload_seq, file_idx),
    };
}

S3Filename S3Filename::newCheckpointManifest(UInt64 store_id, UInt64 upload_seq)
{
    return S3Filename{
        .type = S3FilenameType::CheckpointManifest,
        .store_id = store_id,
        .path = fmt::format("mf_{}", upload_seq),
    };
}


} // namespace DB::S3
