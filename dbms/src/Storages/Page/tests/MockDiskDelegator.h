#pragma once
#include <Storages/PathPool.h>

#include <string>


// TODO: support multi-disk
class MockDiskDelegator : public DB::PSDiskDelegator
{
public:
    MockDiskDelegator(DB::String path_) : path(std::move(path_)) {}

    size_t      numPaths() const { return 1; }
    DB::String  defaultPath() const { return path; }
    DB::String  getPageFilePath(const DB::PageFileIdAndLevel & /*id_lvl*/) const { return path; }
    void        removePageFile(const DB::PageFileIdAndLevel & /*id_lvl*/, size_t /*file_size*/) {}
    DB::Strings listPaths() const
    {
        DB::Strings paths;
        paths.emplace_back(path);
        return paths;
    }
    DB::String choosePath(const DB::PageFileIdAndLevel & /*id_lvl*/) { return path; }
    size_t     addPageFileUsedSize(const DB::PageFileIdAndLevel & /*id_lvl*/,
                                   size_t /*size_to_add*/,
                                   const DB::String & /*pf_parent_path*/,
                                   bool /*need_insert_location*/)
    {
        return 0;
    }

private:
    std::string path;
};
