# Columnar vs TiFlashWrite 读取性能对比指南

本文描述如何在同一集群上对 `readThroughColumnar`（5035）和 `readThroughTiFlashWrite`（5036）两条路径进行单表扫描性能对比。

## 集群拓扑

```
10.2.12.81
├── 5035 (tiflash compute, columnar read, readThroughColumnar)
│   ├── tiflash.log       # C++ pipeline / 算子日志
│   └── tiflash_tikv.log  # Rust FFI / kvengine 日志
└── 5036 (tiflash compute, TiFlashWrite read, readThroughTiFlashWrite)
    ├── tiflash.log       # C++ pipeline / 算子日志
    └── tiflash_tikv.log  # Rust 日志 (columnar 路径不使用)
```

## 发起查询

### 推荐 SQL（含 TSO 输出，方便日志搜索）

```sql
BEGIN;
SELECT @@tidb_current_ts;  -- 输出的 tso 即 tiflash log 中的 start_ts
SET SESSION tiflash_compute_selected_address = "10.2.12.81:9535";  -- 5035 (Columnar)
-- 或 "10.2.12.81:9536"  -- 5036 (TiFlashWrite)
EXPLAIN ANALYZE SELECT count(l_comment) FROM lineitem;
ROLLBACK;
```

`@@tidb_current_ts` 的值就是 TiFlash 日志中 `start_ts` 字段的值。

输出示例：

```
@@tidb_current_ts
466965963161731076           ← 这就是 start_ts
...
TableFullScan_21  300005811  ...  time:1.94s  loops:29385  threads:48
```

### 命令行一行执行（含 TSO）

```bash
# 5035 (Columnar)
mycli -h 10.2.12.81 --port 8030 -D test -u root -e "
BEGIN;
SELECT @@tidb_current_ts;
SET SESSION tiflash_compute_selected_address = '10.2.12.81:9535';
EXPLAIN ANALYZE SELECT count(l_comment) FROM lineitem;
ROLLBACK;
" 2>&1

# 5036 (TiFlashWrite)
mycli -h 10.2.12.81 --port 8030 -D test -u root -e "
BEGIN;
SELECT @@tidb_current_ts;
SET SESSION tiflash_compute_selected_address = '10.2.12.81:9536';
EXPLAIN ANALYZE SELECT count(l_comment) FROM lineitem;
ROLLBACK;
" 2>&1
```

## EXPLAIN ANALYZE 关键指标

| 指标 | 位置 | 含义 |
|---|---|---|
| `time` | `TableFullScan` 行首 | 该算子总耗时 |
| `loops` | `tiflash_task` | 读取的 block 数 |
| `threads` | `tiflash_task` | 管道并发度 |
| `pipeline_queue_wait` | `tiflash_wait` | SharedQueue 等待时间（columnar 特有） |
| `data_scanned_rows` | `tiflash_scan.dtfile` | 从 DM 文件扫描的行数（TiFlashWrite） |
| `query_read_bytes` | `scan_details` | 从存储读取的总字节数 |
| `disagg_cache_hit_bytes` | `scan_details` | S3 缓存命中字节（TiFlashWrite） |
| `dmfile_read_time` | `scan_details` | DM 文件读取总时间（TiFlashWrite） |

## 日志对比

### 用 TSO 直接搜索日志

```bash
TSO=466965963161731076  # 替换为 SELECT @@tidb_current_ts 输出的值

# Columnar (5035) — 查找 C++ 日志
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash.log | head -5
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash.log | tail -5

# Columnar (5035) — 查找 Rust 日志 (需要时间窗口，Rust 日志不含 TSO)
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash.log | head -1 | grep -oP '\d{2}:\d{2}:\d{2}'
# 用上面输出的时间 ±3 秒过滤 Rust 日志
grep "load_pack" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash_tikv.log \
  | awk '/14:32:5[4-7]/' \
  | awk -F'compressed_bytes=' '{s+=$2;c++} END {printf "compressed=%.2fGB packs=%d avg=%.0fB\n", s/1e9, c, s/c}'

# TiFlashWrite (5036)
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5036/log/tiflash.log | grep "MPPTaskStatistics"

### Columnar (5035) — 从 Rust 日志提取 load_pack 统计

```bash
TSO=466965593880264709  # 替换为实际 TSO

# 1. 从 C++ 日志获取查询时间窗口
TIME_START=$(grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash.log | head -1 | grep -oP '\d{2}:\d{2}:\d{2}')
TIME_END=$(grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash.log | tail -1 | grep -oP '\d{2}:\d{2}:\d{2}')
echo "Time window: $TIME_START ~ $TIME_END (use awk pattern like /$TIME_START/,/$TIME_END/)"

# 2. 一行汇总（需根据实际时间调整 awk 的匹配模式）
grep "load_pack" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash_tikv.log \
  | awk "/$TIME_START/,/$TIME_END/" \
  | awk -F'compressed_bytes=' '{csum+=$2; ccount++} END {printf "compressed=%.2fGB packs=%d avg=%.0fB\n", csum/1e9, ccount, csum/ccount}'

grep "load_pack" /data3/jaysonhuang/clusters/tiflash-5035/log/tiflash_tikv.log \
  | awk "/$TIME_START/,/$TIME_END/" \
  | awk -F'decompressed_bytes=' '{dsum+=$2; dcount++} END {printf "decompressed=%.2fGB packs=%d avg=%.0fB\n", dsum/1e9, dcount, dsum/dcount}'
```

`load_pack` 日志格式：

```
load_pack: file=<file_id> pack_offset=<offset> compressed_bytes=<N> decompressed_bytes=<M>
```

### TiFlashWrite (5036) — 从 C++ 日志提取 scan 统计

```bash
TSO=466965632532611077  # 替换为实际 TSO

# MPPTaskStatistics 中包含完整 scan 详情
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5036/log/tiflash.log \
  | grep "MPPTaskStatistics" \
  | python3 -c "
import sys,json
line = sys.stdin.read()
data = json.loads(line.split('] [')[0].split('[\"')[0])
for e in data['executors']:
    if e['type'] == 'TableScan':
        sd = e.get('scan_details',{})
        for k in ['dmfile_read_time','query_read_bytes','disagg_cache_hit_size','disagg_cache_miss_size','num_segments','dmfile_data_scanned_rows']:
            if k in sd: print(f'{k}: {sd[k]}')
        for cd in e.get('connection_details',[]):
            if 'num_streams' in cd and cd['num_streams'] > 0:
                print(f'streams: {cd[\"num_streams\"]}, rows_per_sec: {cd.get(\"rows_per_sec\",\"\")}, bytes_per_sec: {cd.get(\"bytes_per_sec\",\"\")}')
"
```

### SegmentReadTaskPool 日志

```bash
grep "$TSO" /data3/jaysonhuang/clusters/tiflash-5036/log/tiflash.log | grep "SegmentReadTaskPool" \
  | grep -oP 'total_count=\d+ total_bytes=\S+ total_rows=\d+ avg_block_rows=\d+ avg_rows_bytes=\S+'
```

## 正确的对照维度

| 层级 | Columnar (5035) | TiFlashWrite (5036) |
|---|---|---|
| **Reader 抽象** | 171 个 `ColumnarReader` (reader works) | 341 个 `Segment` (DM segments) |
| **Block 读取** | 29,385 次 `fn_read_block` FFI 调用 | 4,782 次 `SegmentReadTask` block read |
| **存储 IO 单元** | 208,734 次 `load_pack` (每 pack ~53KB 压缩) | `dmfile_read_time` (大块连续 IO) |
| **存储读取量** | `total_compressed_bytes` (load_pack 日志汇总) | `query_read_bytes` (scan_details) |
| **存储缓存** | 列存 segment cache (256KB 粒度) | `disagg_cache_hit_size` (S3 对象级) |
| **并行度** | 48 (`producer_num`) | 72 (`num_streams`) |
| **Pipeline 模型** | 双 group: 48 producer → SharedQueue → 72 consumer | 单 group: 72 source 直读 |

### EXPLAIN ANALYZE 指标对照表

| 指标 | Columnar | TiFlashWrite |
|---|---|---|
| 总查询时间 | `time` on TableFullScan | `time` on TableFullScan |
| Block 数量 | `loops` in tiflash_task | `loops` in tiflash_task |
| 扫描行数 | `actRows` on TableFullScan | `dmfile_data_scanned_rows` in scan_details |
| 管道并发 | `threads` in tiflash_task | `num_streams` in connection_details |
| 队列等待 | `pipeline_queue_wait` | N/A |
| 读字节量 | Rust log `load_pack` 汇总 | `query_read_bytes` in scan_details |
| S3 缓存 | 暂无 (Rust 侧 segment cache) | `disagg_cache_hit_size` |

## 示例：lineitem 300M 行 count(l_comment)

| 指标 | Columnar | TiFlashWrite | 比值 |
|---|---|---|---|
| TableScan 时间 | ~1.9s | ~370ms | 5.1x |
| Block 数 | 29,385 | 4,782 | 6.1x |
| Rows/block | ~10K | ~63K | 0.16x |
| 存储读取量 | ~10.3 GB | ~10.65 GB | 0.97x |
| Pack/Segment 数 | 208,734 | 341 | 612x |
| Queue wait | ~200ms | ~20ms | — |

## 注意事项

1. **冷热启动差异**：Columnar 首次查询可能 9s+（reader materialize + pack 首次加载），后续查询 ~2s（reader 已就绪 + pack 已缓存）
2. **日志匹配**：使用 `SELECT @@tidb_current_ts` 获取的 TSO 直接搜索 C++ 日志。`load_pack` 日志来自 `tiflash_tikv.log`（不含 TSO），需要根据 C++ 日志中的时间窗口过滤 Rust 侧日志
3. **单列 vs 多列**：以上对比使用 `count(l_comment)`（单列），多列查询的 `load_pack` 次数会按列数倍增
4. **Server 模式**：Columnar 需确认 `use_columnar=true` 且 `ENABLE_NEXT_GEN_COLUMNAR=1`；TiFlashWrite 需确认 `use_columnar=false`
