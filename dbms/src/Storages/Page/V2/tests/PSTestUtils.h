#pragma once

#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V2/PageStorage.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

namespace DB
{
namespace PS::V2::tests
{
String entryToString(const PageEntry & entry)
{
    return fmt::format("PageEntry{{file_id_lvl: <{},{}>, offset: {}, size: {}}}", entry.file_id, entry.level, entry.offset, entry.size);
}

::testing::AssertionResult checkEntryNotExist(const char * snap_expr, const char * page_id_expr, PageStorage::ConcreteSnapshotPtr & snap, PageId pid)
{
    auto entry = snap->version()->find(pid);
    if (!entry)
        return ::testing::AssertionSuccess();
    auto error = fmt::format("Expect entry {}->entry[{}] not existed, but existed", snap_expr, page_id_expr);
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define EXPECT_NO_ENTRY(snap, page_id) EXPECT_PRED_FORMAT2(checkEntryNotExist, snap, page_id);

::testing::AssertionResult checkEntryAt(const char * snap_expr, const char * page_id_expr, const char * id_lvl_expr, PageStorage::ConcreteSnapshotPtr & snap, PageId pid, const DB::PageFileIdAndLevel & input_id_lvl)
{
    auto entry = snap->version()->find(pid);
    if (!entry)
    {
        auto error = fmt::format("Expect found entry from {}->entry[{}], but not existed!", snap_expr, page_id_expr);
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    if ((*entry).fileIdLevel() == input_id_lvl)
        return ::testing::AssertionSuccess();
    auto expect_expr = fmt::format("{}->entry[{}].fileIdLevel", snap_expr, page_id_expr);
    return ::testing::internal::EqFailure(expect_expr.c_str(), id_lvl_expr, fmt::format("<{},{}>", entry->file_id, entry->level), fmt::format("<{},{}>", input_id_lvl.first, input_id_lvl.second), false);
}
#define EXPECT_ENTRY_EXIST(snap, page_id, expected_entry) EXPECT_PRED_FORMAT3(checkEntryAt, snap, page_id, expected_entry)

} // namespace PS::V2::tests
} // namespace DB
