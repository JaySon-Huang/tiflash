#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

#include "gtest/gtest.h"

namespace DB::PS::V3::tests
{
String toString(const PageEntryV3 & entry)
{
    return fmt::format("PageEntry{{file: {}, offset: 0x{:X}, size: {}, checksum: 0x{:X}}}", entry.file_id, entry.offset, entry.size, entry.checksum);
}

::testing::AssertionResult PageEntryCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const PageEntryV3 & lhs,
    const PageEntryV3 & rhs)
{
    // Maybe need more field check later
    if (lhs.file_id == rhs.file_id && lhs.offset == rhs.offset && lhs.size == rhs.size)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, toString(lhs), toString(rhs), false);
}
#define ASSERT_ENTRY_EQ(val1, val2) ASSERT_PRED_FORMAT2(PageEntryCompare, val1, val2)

class PageDirectoryTest : public ::testing::Test
{
protected:
    PageDirectory dir;

    PageEntryV3 get(PageId page_id, const PageDirectorySnapshotPtr & snap) const
    {
        auto [res, id_entry] = dir.safeGet(page_id, snap);
        auto & [pid, entry] = id_entry;
        if (pid == page_id)
            return entry_got;
        throw Exception(fmt::format("Try to get Page{} but got Page{}", page_id, pid), ErrorCodes::LOGICAL_ERROR);
    }
};

TEST_F(PageDirectoryTest, ApplyRead)
{
    PageEntriesEdit edit;
    PageEntryV3 entry1{.file_id = 1, .size = 1024, .offset = 0x123, .checksum = 0x4567};
    edit.put(1, entry1);

    auto snap = dir.createSnapshot();
    dir.apply(std::move(edit));
    ASSERT_ENTRY_EQ(entry1, get(1, snap));
}

} // namespace DB::PS::V3::tests
