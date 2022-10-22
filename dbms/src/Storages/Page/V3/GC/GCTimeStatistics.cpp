#include <Common/TiFlashMetrics.h>
#include <Storages/Page/V3/GC/GCTimeStatistics.h>
#include <fmt/format.h>

namespace DB::PS::V3
{

String GCTimeStatistics::toLogging() const
{
    const std::string_view stage_suffix = [this]() {
        switch (stage)
        {
        case GCStageType::Unknown:
            return " <unknown>";
        case GCStageType::OnlyInMem:
            return " without full gc";
        case GCStageType::FullGCNothingMoved:
            return " without moving any entry";
        case GCStageType::FullGC:
            return "";
        }
    }();
    const auto get_external_msg = [this]() -> String {
        if (clean_external_page_ms == 0)
            return String("");
        static constexpr double SCALE_NS_TO_MS = 1'000'000.0;
        return fmt::format(" [external_callbacks={}] [external_gc={}ms] [scanner={:.2f}ms] [get_alive={:.2f}ms] [remover={:.2f}ms]",
                           num_external_callbacks,
                           clean_external_page_ms,
                           external_page_scan_ns / SCALE_NS_TO_MS,
                           external_page_get_alive_ns / SCALE_NS_TO_MS,
                           external_page_remove_ns / SCALE_NS_TO_MS);
    };
    return fmt::format("GC finished{}."
                       " [total time={}ms]"
                       " [compact wal={}ms] [compact directory={}ms] [compact spacemap={}ms]"
                       " [gc status={}ms] [gc entries={}ms] [gc data={}ms]"
                       " [gc apply={}ms]"
                       "{}", // a placeholder for external page gc at last
                       stage_suffix,
                       total_cost_ms,
                       compact_wal_ms,
                       compact_directory_ms,
                       compact_spacemap_ms,
                       full_gc_prepare_ms,
                       full_gc_get_entries_ms,
                       full_gc_blobstore_copy_ms,
                       full_gc_apply_ms,
                       get_external_msg());
}

void GCTimeStatistics::finishCleanExternalPage(UInt64 clean_cost_ms)
{
    clean_external_page_ms = clean_cost_ms;
    GET_METRIC(tiflash_storage_page_gc_duration_seconds, type_clean_external).Observe(clean_external_page_ms / 1000.0);
}
} // namespace DB::PS::V3
