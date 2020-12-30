#include <Interpreters/Context.h>
#include <Storages/IManageableStorage.h>
#include <Storages/Transaction/BackgroundService.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionDataMover.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

BackgroundService::BackgroundService(TMTContext & tmt_)
    : tmt(tmt_), background_pool(tmt.getContext().getBackgroundPool()), log(&Logger::get("BackgroundService"))
{
    if (!tmt.isInitialized())
        throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

    single_thread_task_handle = background_pool.addTask(
        [this] {
            {
                RegionTable & region_table = tmt.getRegionTable();
                region_table.checkTableOptimize();
            }
            tmt.getKVStore()->gcRegionCache();
            return false;
        },
        false);

    if (!tmt.isBgFlushDisabled())
    {
        table_flush_handle = background_pool.addTask([this] {
            RegionTable & region_table = tmt.getRegionTable();

            // if all regions of table is removed, try to optimize data.
            if (auto table_id = region_table.popOneTableToOptimize(); table_id != InvalidTableID)
            {
                LOG_INFO(log, "try to final optimize table " << table_id);
                tryOptimizeStorageFinal(tmt.getContext(), table_id);
                LOG_INFO(log, "finish final optimize table " << table_id);
            }
            return region_table.tryFlushRegions();
        });

        region_handle = background_pool.addTask([this] {
            bool ok = false;
            {
                RegionPtr region = nullptr;
                {
                    std::lock_guard<std::mutex> lock(region_mutex);
                    if (!regions_to_decode.empty())
                    {
                        auto it = regions_to_decode.begin();
                        region = it->second;
                        regions_to_decode.erase(it);
                        ok = true;
                    }
                }
                if (region)
                    region->tryPreDecodeTiKVValue(tmt);
            }

            {
                RegionPtr region = nullptr;
                {
                    std::lock_guard<std::mutex> lock(region_mutex);
                    if (!regions_to_flush.empty())
                    {
                        auto it = regions_to_flush.begin();
                        region = it->second;
                        regions_to_flush.erase(it);
                        ok = true;
                    }
                }
                if (region)
                    tmt.getRegionTable().tryFlushRegion(region, true);
            }
            return ok;
        });

        {
            std::vector<RegionPtr> regions;
            tmt.getKVStore()->traverseRegions([&regions](RegionID, const RegionPtr & region) {
                if (region->dataSize())
                    regions.emplace_back(region);
            });

            for (const auto & region : regions)
                addRegionToDecode(region);
        }
    }
    else
    {
        LOG_INFO(log, "Configuration raft.disable_bg_flush is set to true, background flush tasks are disabled.");
        storage_gc_handle = background_pool.addTask(
            [this] {
                // Get a storage snapshot with weak_ptrs first
                std::unordered_map<TableID, std::weak_ptr<IManageableStorage>> storages;
                for (const auto & [table_id, storage] : tmt.getStorages().getAllStorage())
                    storages.emplace(table_id, storage);
                for (const auto & [table_id, storage_] : storages)
                {
                    // The TiFlash process receive a signal to terminate.
                    if (tmt.getTerminated())
                        break;

                    std::ignore = table_id;
                    auto storage = storage_.lock();
                    // The storage has been free or dropped.
                    if (!storage || storage->is_dropped)
                        continue;
                    try
                    {
                        // Block this thread and do GC on the storage
                        // It is OK if any schema changes is apply to the storage while doing GC, so we
                        // do not acquire structure lock on the storage.
                        storage->onSyncGc(tmt.getContext());
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
                // Always return false
                return false;
            },
            false, /*interval_ms=*/5 * 60 * 1000);
    }
}

void BackgroundService::addRegionToDecode(const RegionPtr & region)
{
    if (tmt.isBgFlushDisabled())
        throw Exception("Try to addRegionToDecode while background flush is disabled.", ErrorCodes::LOGICAL_ERROR);

    {
        std::lock_guard<std::mutex> lock(region_mutex);
        regions_to_decode.emplace(region->id(), region);
    }
    region_handle->wake();
}

void BackgroundService::addRegionToFlush(const DB::RegionPtr & region)
{
    if (tmt.isBgFlushDisabled())
        throw Exception("Try to addRegionToFlush while background flush is disabled.", ErrorCodes::LOGICAL_ERROR);

    {
        std::lock_guard<std::mutex> lock(region_mutex);
        regions_to_flush.emplace(region->id(), region);
    }
    region_handle->wake();
}

BackgroundService::~BackgroundService()
{
    if (single_thread_task_handle)
    {
        background_pool.removeTask(single_thread_task_handle);
        single_thread_task_handle = nullptr;
    }
    if (table_flush_handle)
    {
        background_pool.removeTask(table_flush_handle);
        table_flush_handle = nullptr;
    }

    if (region_handle)
    {
        background_pool.removeTask(region_handle);
        region_handle = nullptr;
    }
}

} // namespace DB
