#pragma once

#include <common/types.h>

namespace DB::PS::V3
{
enum class GCStageType
{
    Unknown,
    OnlyInMem,
    FullGCNothingMoved,
    FullGC,
};
struct GCTimeStatistics
{
    GCStageType stage = GCStageType::Unknown;
    bool executeNextImmediately() const { return stage == GCStageType::FullGC; };

    UInt64 total_cost_ms = 0;

    UInt64 compact_wal_ms = 0;
    UInt64 compact_directory_ms = 0;
    UInt64 compact_spacemap_ms = 0;
    // Full GC
    UInt64 full_gc_prepare_ms = 0;
    UInt64 full_gc_get_entries_ms = 0;
    UInt64 full_gc_blobstore_copy_ms = 0;
    UInt64 full_gc_apply_ms = 0;

    // GC external page
    UInt64 num_external_callbacks = 0;
    // Breakdown the duration for cleaning external pages
    // ms is usually too big for these operation, store by ns (10^-9)
    UInt64 external_page_scan_ns = 0;
    UInt64 external_page_get_alive_ns = 0;
    UInt64 external_page_remove_ns = 0;
    // Total time of cleaning external pages
private:
    UInt64 clean_external_page_ms = 0;

public:
    void finishCleanExternalPage(UInt64 clean_cost_ms);

    String toLogging() const;
};
} // namespace DB::PS::V3
