// Only enable these tests under debug mode because we need some classes under `MockUtils.h`
#include "gtest/internal/gtest-internal.h"
#ifndef NDEBUG

#include <Common/FailPoint.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V2/gc/DataCompactor.h>
#include <Storages/Page/V2/mock/MockUtils.h>
#include <Storages/PathPool.h>
#include <TestUtils/MockDiskDelegator.h>
#include <TestUtils/TiFlashTestBasic.h>

using DB::tests::TiFlashTestEnv;

namespace DB
{
namespace FailPoints
{
extern const char force_formal_page_file_not_exists[];
extern const char force_legacy_or_checkpoint_page_file_exists[];
} // namespace FailPoints
namespace PS::V2::tests
{
// #define GENERATE_TEST_DATA

String entryToString(const PageEntry & entry)
{
    return fmt::format("PageEntry{{file_id_lvl: <{},{}>, offset: {}, size: {}}}", entry.file_id, entry.level, entry.offset, entry.size);
}


::testing::AssertionResult checkEntryNotExist(const char *snap_expr, const char * page_id_expr, PageStorage::ConcreteSnapshotPtr & snap, PageId pid)
{
    auto entry = snap->version()->find(pid);
    if (!entry)
        return ::testing::AssertionSuccess();
    auto error = fmt::format("Expect entry {}->getEntry({}) not existed, but existed", snap_expr, page_id_expr);
    return ::testing::AssertionFailure(::testing::Message(error.c_str()));
}
#define EXPECT_NO_ENTRY(snap, page_id) EXPECT_PRED_FORMAT2(checkEntryNotExist, snap, page_id);

::testing::AssertionResult checkEntryAt(const char * snap_expr, const char * page_id_expr, const char * id_lvl_expr, PageStorage::ConcreteSnapshotPtr & snap, PageId pid, const DB::PageFileIdAndLevel & input_id_lvl)
{
    auto entry = snap->version()->find(pid);
    if (!entry)
    {
        auto error = fmt::format("Expect found entry from {}->getEntry({}), but not existed!", snap_expr, page_id_expr);
        return ::testing::AssertionFailure(::testing::Message(error.c_str()));
    }
    if ((*entry).fileIdLevel() == input_id_lvl)
        return ::testing::AssertionSuccess();
    auto expect_expr = fmt::format("{}->getEntry({}).fileIdLevel", snap_expr, page_id_expr);
    return ::testing::internal::EqFailure(expect_expr.c_str(), id_lvl_expr, fmt::format("<{},{}>", entry->file_id, entry->level), fmt::format("<{},{}>", input_id_lvl.first, input_id_lvl.second), false);
}

#define EXPECT_ENTRY_EXIST(snap, page_id, expected_entry) EXPECT_PRED_FORMAT3(checkEntryAt, snap, page_id, expected_entry)

TEST(DataCompactorTest, MigratePages)
try
{
    CHECK_TESTS_WITH_DATA_ENABLED;

    PageStorage::Config config;
    config.num_write_slots = 2;
#ifndef GENERATE_TEST_DATA
    const Strings test_paths = TiFlashTestEnv::findTestDataPath("page_storage_compactor_migrate");
    ASSERT_EQ(test_paths.size(), 2);
#else
    const String test_path = TiFlashTestEnv::getTemporaryPath("page_storage_compactor_migrate");
    if (Poco::File f(test_path); f.exists())
        f.remove(true);
    const Strings test_paths = Strings{
        test_path + "/data0",
        test_path + "/data1",
    };
#endif

    auto ctx = TiFlashTestEnv::getContext(DB::Settings());
    const auto file_provider = ctx.getFileProvider();
    PSDiskDelegatorPtr delegate = std::make_shared<DB::tests::MockDiskDelegatorMulti>(test_paths);

    PageStorage storage("data_compact_test", delegate, config, file_provider);

#ifdef GENERATE_TEST_DATA
    // Codes to generate a directory of test data
    storage.restore();
    // Created by these write batches:
    {
        char i = 0;
        char buf[1024] = {'\0'};
        auto create_buff_ptr = [&buf, &i](size_t sz) -> ReadBufferPtr {
            buf[0] = i++;
            return std::make_shared<ReadBufferFromMemory>(buf, sz);
        };

        const size_t page_size = 1;
        {
            // This is written to PageFile{1, 0}
            WriteBatch wb;
            wb.putPage(1, 0, create_buff_ptr(page_size), page_size); // page 1, data 0
            storage.write(std::move(wb));
        }
        {
            // This is written to PageFile{2, 0}
            WriteBatch wb;
            wb.putPage(1, 1, create_buff_ptr(page_size), page_size); // new version of page 1, data 1
            wb.putPage(2, 0, create_buff_ptr(page_size), page_size); // page 2, data 2
            wb.putRefPage(3, 2); // page 3 -ref-> page 2
            wb.putPage(4, 0, create_buff_ptr(page_size), page_size); // page 4, data 3
            storage.write(std::move(wb));
        }
        {
            // This is written to PageFile{1, 0}
            WriteBatch wb;
            wb.putPage(1, 2, create_buff_ptr(page_size), page_size); // new version of page 1, data 4
            wb.delPage(4); // del page 4
            wb.putRefPage(5, 3); // page 5 -ref-> page 3 --> page 2
            wb.delPage(3); // del page 3, page 5 -ref-> page 2
            wb.putPage(6, 0, create_buff_ptr(page_size), page_size); // page 6, data 5
            storage.write(std::move(wb));
        }
        return;
    }
#endif

    // snapshot contains {1, 2, 6}
    // Not contains 3, 4 since it's deleted, 5 is a ref to 2.
    auto snapshot = MockSnapshot::createFrom({
        // pid, entry
        {1, PageEntry{.file_id = 1}},
        {2, PageEntry{.file_id = 2}},
        {6, PageEntry{.file_id = 1}},
    });

    // valid_pages
    DataCompactor<MockSnapshotPtr> compactor(storage, config, nullptr, nullptr);
    auto valid_pages = DataCompactor<MockSnapshotPtr>::collectValidPagesInPageFile(snapshot);
    ASSERT_EQ(valid_pages.size(), 2); // 3 valid pages in 2 PageFiles

    auto candidates = PageStorage::listAllPageFiles(file_provider, delegate, storage.page_file_log);
    const PageFileIdAndLevel target_id_lvl{2, 1};
    {
        // Apply migration
        auto [edits, bytes_written] = compactor.migratePages(
            snapshot,
            valid_pages,
            DataCompactor<MockSnapshotPtr>::CompactCandidates{candidates, PageFileSet{}, PageFileSet{}, 0, 0},
            0);
        std::ignore = bytes_written;
        ASSERT_EQ(edits.size(), 3); // page 1, 2, 6
        auto & records = edits.getRecords();
        for (size_t i = 0; i < records.size(); ++i)
        {
            const auto & rec = records[i];
            EXPECT_EQ(rec.type, WriteBatch::WriteType::UPSERT);
            // Page 1, 2, 6 is moved to PageFile{2,1}
            if (rec.page_id == 1 || rec.page_id == 2 || rec.page_id == 6)
            {
                EXPECT_EQ(rec.entry.fileIdLevel(), target_id_lvl);
            }
            else
                GTEST_FAIL() << "unknown page_id: " << rec.page_id;
        }
    }

    for (size_t i = 0; i < delegate->numPaths(); ++i)
    {
        // Try to apply migration again, should be ignore because PageFile_2_1 exists
        size_t bytes_written = 0;
        std::tie(std::ignore, bytes_written) = compactor.migratePages(
            snapshot,
            valid_pages,
            DataCompactor<MockSnapshotPtr>::CompactCandidates{candidates, PageFileSet{}, PageFileSet{}, 0, 0},
            0);
        ASSERT_EQ(bytes_written, 0) << "should not apply migration";
    }

    for (size_t i = 0; i < delegate->numPaths(); ++i)
    {
        // Mock that PageFile_2_1 have been "Legacy", try to apply migration again, should be ignore because legacy.PageFile_2_1 exists
        FailPointHelper::enableFailPoint(FailPoints::force_formal_page_file_not_exists);
        FailPointHelper::enableFailPoint(FailPoints::force_legacy_or_checkpoint_page_file_exists);
        size_t bytes_written = 0;
        std::tie(std::ignore, bytes_written) = compactor.migratePages(
            snapshot,
            valid_pages,
            DataCompactor<MockSnapshotPtr>::CompactCandidates{candidates, PageFileSet{}, PageFileSet{}, 0, 0},
            0);
        ASSERT_EQ(bytes_written, 0) << "should not apply migration";
    }

    {
        // Try to recover from disk, check whether page 1, 2, 3, 4, 5, 6 is valid or not.
        PageStorage ps("data_compact_test", delegate, config, file_provider);
        ps.restore();
        // Page 1, 2 have been migrated to PageFile_2_1
        auto snap = ps.getConcreteSnapshot();
        EXPECT_ENTRY_EXIST(snap, 1, target_id_lvl);
        EXPECT_ENTRY_EXIST(snap, 2, target_id_lvl);

        // Page 5 -ref-> 2
        auto entry2 = snap->version()->find(2);
        auto entry5 = snap->version()->find(5);
        EXPECT_EQ(*entry2, *entry5);

        // Page 3, 4 are deleted
        EXPECT_NO_ENTRY(snap, 3);
        EXPECT_NO_ENTRY(snap, 4);

        // Page 6 have been migrated to PageFile_2_1
        EXPECT_ENTRY_EXIST(snap, 6, target_id_lvl);
    }
}
CATCH

} // namespace PS::V2::tests
} // namespace DB

#endif // NDEBUG
