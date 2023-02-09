
#include <Storages/S3Filename.h>
#include <gtest/gtest.h>

namespace DB::S3::tests
{
TEST(S3FilenameTest, Manifest)
{
    UInt64 test_store_id = 1027;
    UInt64 test_seq = 20;
    String fullkey = "s1027/manifest/mf_20";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::CheckpointManifest);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.path, "mf_20");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_FALSE(view.isDataFile());
        ASSERT_FALSE(view.isLockFile());

        ASSERT_EQ(view.getUploadSequence(), test_seq);
    };

    auto view = S3FilenameView::fromKey(fullkey);
    check(view);

    {
        auto r = S3Filename::newCheckpointManifest(test_store_id, test_seq);
        ASSERT_EQ(r.toFullKey(), fullkey);
        check(r.toView());
    }
}

TEST(S3FilenameTest, CheckpointDataFile)
{
    UInt64 test_store_id = 2077;
    UInt64 test_seq = 99;
    UInt64 test_file_idx = 1;
    String fullkey = "s2077/data/dat_99_1";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::CheckpointDataFile);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.path, "dat_99_1");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_TRUE(view.isDataFile());
        ASSERT_EQ(view.getLockKey(1234, 50), "s2077/lock/dat_99_1.lock_s1234_50");
        ASSERT_EQ(view.getDelMarkKey(), "s2077/data/dat_99_1.del");
        ASSERT_EQ(view.getUploadSequence(), test_seq);

        ASSERT_FALSE(view.isLockFile());
    };

    auto view = S3FilenameView::fromKey(fullkey);
    check(view);

    auto r = S3Filename::newCheckpointData(test_store_id, test_seq, test_file_idx);
    ASSERT_EQ(r.toFullKey(), fullkey);
    check(r.toView());
}

TEST(S3FilenameTest, StableFile)
{
    UInt64 test_store_id = 2077;
    String fullkey = "s2077/stable/t_44/dmf_57";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::StableFile);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.path, "t_44/dmf_57");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_TRUE(view.isDataFile());
        ASSERT_EQ(view.getLockKey(1234, 50), "s2077/lock/t_44/dmf_57.lock_s1234_50");
        ASSERT_EQ(view.getDelMarkKey(), "s2077/stable/t_44/dmf_57.del");

        ASSERT_FALSE(view.isLockFile());
    };
    auto view = S3FilenameView::fromKey(fullkey);
    check(view);
}
} // namespace DB::S3::tests
