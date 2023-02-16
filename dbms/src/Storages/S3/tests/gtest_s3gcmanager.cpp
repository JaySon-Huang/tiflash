#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3GCManager.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::S3
{

class S3GCManagerTest : public ::testing::Test
{
public:
    static void SetUpTestCase()
    {
        ClientFactory::instance().init(true);
    }
};

TEST_F(S3GCManagerTest, Simple)
try
{
    S3GCManager gc_mgr("/tmp");
    gc_mgr.runOnAllStores();
}
CATCH

} // namespace DB::S3
