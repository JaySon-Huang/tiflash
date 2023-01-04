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

#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>

#include <boost/noncopyable.hpp>

namespace DB::DM::Remote
{

class IPreparedDMFileToken : private boost::noncopyable
{
protected:
    // These should be the required information for any kind of DataStore.
    const FileProviderPtr file_provider;
    const DMFileOID oid;

    explicit IPreparedDMFileToken(const FileProviderPtr & file_provider_, const DMFileOID & oid_)
        : file_provider(file_provider_)
        , oid(oid_)
    {}

public:
    virtual ~IPreparedDMFileToken() = default;

    /**
     * Restores into a DMFile object. This token will be kept valid when DMFile is valid.
     */
    virtual DMFilePtr restore(DMFile::ReadMetaMode read_mode) = 0;
};

using IPreparedDMFileTokenPtr = std::shared_ptr<IPreparedDMFileToken>;

class LocalCachePreparedDMFileToken : public IPreparedDMFileToken
{
protected:
    const String local_cache_parent_directory;

    explicit LocalCachePreparedDMFileToken(const FileProviderPtr & file_provider_, const DMFileOID & oid_, const String & local_cache_parent_directory_)
        : IPreparedDMFileToken(file_provider_, oid_)
        , local_cache_parent_directory(local_cache_parent_directory_)
    {}

public:
    /**
     * Restores into a DMFile object. This token will be kept valid when DMFile is valid.
     */
    DMFilePtr restore(DMFile::ReadMetaMode read_mode) override
    {
        return DMFile::restore(
            file_provider,
            oid.file_id,
            /*page_id*/ oid.file_id,
            local_cache_parent_directory,
            read_mode);
    }
};

class IDataStore : private boost::noncopyable
{
public:
    virtual ~IDataStore() = default;

    /**
     * Blocks until a local DMFile is successfully put in the remote data store.
     * Should be used by a write node.
     */
    virtual void putDMFile(DMFilePtr local_dm_file, const DMFileOID & oid) = 0;

    virtual void copyDMFileMetaToLocalPath(const DMFileOID & remote_oid, const String & local_path) = 0;

    virtual void linkDMFile(const DMFileOID & remote_oid, const DMFileOID & self_oid) = 0;

    /**
     * Blocks until a DMFile in the remote data store is successfully prepared in a local cache.
     * If the DMFile exists in the local cache, it will not be prepared again.
     *
     * Returns a "token", which can be used to rebuild the `DMFile` object.
     * The DMFile in the local cache may be invalidated if you deconstructs the token.
     *
     * Should be used by a read node.
     */
    virtual IPreparedDMFileTokenPtr prepareDMFile(const DMFileOID & oid) = 0;


    // Define the key format for S3 objects.
    static String getDTFileRemoteFullPath(const DMFileOID & oid)
    {
        return fmt::format("s{}/stable/{}", oid.write_node_id, getDTFileWithTable(oid));
    }

    static String getDTFileLockRemoteFullPath(const DMFileOID & oid, const UInt64 lock_store_id, UInt64 lock_seq)
    {
        return fmt::format("s{}/lock/{}.lock_s{}_{}", oid.write_node_id, getDTFileWithTable(oid), lock_store_id, lock_seq);
    }

    static std::optional<DMFileOID> parseFromFullpath(const String & fullpath)
    {
        DMFileOID oid;
        sscanf(fullpath.c_str(), "s%lu/stable/t_%ld/dmf_%lu", &oid.write_node_id, &oid.table_id, &oid.file_id);
        return oid;
    }

private:
    static String getDTFileWithTable(const DMFileOID & oid)
    {
        return fmt::format("t_{}/dmf_{}", oid.table_id, oid.file_id);
    }
};

using IDataStorePtr = std::shared_ptr<IDataStore>;

} // namespace DB::DM::Remote
