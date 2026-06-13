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

## 并发读模型

### Columnar (readThroughColumnar)

```
                    Pipeline IO Pool (72 threads, TaskScheduler)
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
     ColumnarReadSourceOp (×48, producer_num)
     │  SourceOp，但 awaitImpl() 返回 IO_IN → executeIOImpl() 在 IO pool 中执行
     │
     │  executeIOImpl():
     │    1. acquire reader work (内联 materialize 或消费 prefetch 已完成的 reader)
     │    2. fn_read_block → fn_read_handle → fn_read_column (FFI)
     │    3. 列反序列化 → 填充 extra table id → 产出 1 个 Block
     │  readImpl():
     │    将缓存的 block 交给下游 SharedQueueSinkOp (CPU pool)
     │
     │  [prefetch — 也走 IO pool]
     │  tryAcquireReaderWork(enable_prefetch=true)
     │    → prefetchPendingWork()
     │      → TaskScheduler::submit(PrefetchColumnarReaderTask)
     │        → IO pool → fn_get_columnar_reader → 设 state=Ready → notify
     │
     └──────────────┬──────────────┘
                    │ tryPush
          ┌─────────▼─────────┐
          │   SharedQueue     │  cap = producer_num × 2, LooseBoundedMPMCQueue<Block>
          │  (反压: full → SharedQueueSinkOp → WAIT_FOR_NOTIFY)  │
          │  (空:  → consumer → WAIT_FOR_NOTIFY)                 │
          └─────────┬─────────┘
                    │ tryPop
     ┌──────────────┼──────────────┐
     ▼              ▼              ▼
  RNColumnarSourceOp (×72, source_num)
  │  readImpl() → shared_queue->tryPop(block)
  │    → EMPTY:   setNotifyFuture + WAIT_FOR_NOTIFY
  │    → OK:      HAS_OUTPUT
  │    → FINISHED: 空 block (EOF)
  │  (无 executeIOImpl / awaitImpl — 纯 CPU pool 消费)
  │
  └──► GeneratedColumn → extraCast → filter → projection

  Pipeline CPU Pool (72 threads)
```

| 组件 | 职责 | 执行位置 |
|---|---|---|
| `ColumnarReadSourceOp` | reader materialize + fn_read_block + deserialize + push | **IO pool** (executeIOImpl) |
| `SharedQueueSinkOp` | tryPush → full 时 WAIT_FOR_NOTIFY | CPU pool (writeImpl) |
| `SharedQueue` | 有界 MPMC 队列，反压通道 | — |
| `RNColumnarSourceOp` | tryPop → emit block | **CPU pool** (readImpl) |
| `PrefetchColumnarReaderTask` | 异步 materialize reader | **IO pool** (via TaskScheduler) |

### TiFlashWrite (readThroughTiFlashWrite)

```
     SegmentReadTaskScheduler (1 sched_thread, 全局单例)
     │  schedule() → scheduleOneRound()
     │    → scheduleMergedTask()              [合并同一 segment 的多个 pool 请求]
     │      → push to MergedTaskPool
     │
     └──► SegmentReaderPool (72 read threads, NUMA-aware, 独立于 Pipeline)
          │
          ├─ pop MergedTask from segment read queue
          │
          ├─ MergedTask::readBlock()
          │    → initOnce()
          │       → pool->buildInputStream(task)    [创建 DMFileReader stream]
          │    → readOneBlock()
          │       → stream->read()                  [DMFile 读一个 block]
          │       → pool->readOneBlock()            [调用 SegmentReadTaskPool::readOneBlock]
          │          → pool->pushBlock(block)       [push to WorkQueue<Block>]
          │             → q.push(block)             [通知等待的 UnorderedSourceOp]
          │
          └──────────────┬──────────────┘
                         │ push
               ┌─────────▼──────────┐
               │  WorkQueue<Block>  │  slot_limit × num_streams
               │  (LooseBoundedMPMC)│  SegmentReadTaskPool 内部队列
               │  (反压: 通过 block_slot_limit + active_segment_limit)  │
               └─────────┬──────────┘
                         │ tryPopBlock
     ┌───────────────────┼───────────────────┐
     ▼                   ▼                   ▼
  UnorderedSourceOp (×72, num_streams)
  │  readImpl() → task_pool->tryPopBlock(block)
  │    → false (空):  setNotifyFuture(task_pool.get()) + WAIT_FOR_NOTIFY
  │    → true, has block: HAS_OUTPUT
  │    → true, null block: done (EOF)
  │  (无 executeIOImpl / awaitImpl — 纯 CPU pool 消费)
  │
  └──► GeneratedColumn → extraCast → filter → projection

  Pipeline CPU Pool (72 threads)
```

| 组件 | 职责 | 执行位置 |
|---|---|---|
| `SegmentReadTaskScheduler` | 调度 segment 读请求，合并同 segment pool | 独立 sched_thread |
| `SegmentReaderPool` | 驱动 MergedTask 读取 DM 文件 | **独立 read thread pool** (非 Pipeline) |
| `MergedTask` | 合并多个 pool 对同一 segment 的读，共享 stream->read() | SegmentReaderPool 线程内 |
| `stream` (DMFileReader) | 实际执行 DM 文件 IO 和 block 产出 | SegmentReaderPool 线程内 |
| `SegmentReadTaskPool` / `WorkQueue<Block>` | 接收 IO 产出的 block，通知 consumer | — |
| `UnorderedSourceOp` | tryPop → emit block | **CPU pool** (readImpl) |

### 关键差异

| 维度 | Columnar | TiFlashWrite |
|---|---|---|
| **IO 线程** | Pipeline **IO pool** (72 threads, TaskScheduler 管理) | **SegmentReaderPool** (72 threads, 独立 NUMA-aware, 非 Pipeline) |
| **CPU 线程** | Pipeline **CPU pool** → `RNColumnarSourceOp` | Pipeline **CPU pool** → `UnorderedSourceOp` |
| **中间队列** | `SharedQueue` (跨 group 连接) | `WorkQueue<Block>` (SegmentReadTaskPool 内部) |
| **IO 执行者** | `ColumnarReadSourceOp` (Pipeline SourceOp) | `MergedTask` → `DMFileReader` stream (非 Pipeline) |
| **调度器** | Pipeline TaskScheduler | `SegmentReadTaskScheduler` + `SegmentReaderPool` |
| **IO 工作** | fn_read_block FFI → load_pack → LZ4 解压 → 列反序列化 | DMFileReader → 本地磁盘/S3 读 DM block |
| **反压** | SharedQueue cap + WAIT_FOR_NOTIFY | `block_slot_limit` + `active_segment_limit` |
| **Prefetch** | `TaskScheduler::submit(PrefetchColumnarReaderTask)` | 不需要 (segment reader 持续驱动 IO) |

## 单 Segment / 单 ColumnarReader 内部读取过程

### TiFlashWrite: 单 Segment 读取

```
segment->getInputStream()
  │
  ├─ 1. prepareMVCCIndex()
  │      预取 delta index / version chain 索引
  │      加速后续 MVCC 判断
  │
  ├─ 2. getReadInfo()
  │      确定需要读哪些列 (columns_to_read + handle + version)
  │
  ├─ 3. 构建 stream 链:
  │     │
  │     ├─ getPlacedStream()
  │     │     ├─ StableValueSpace: DMFileBlockInputStream
  │     │     │    读 DM 文件中指定列的 block (handle + version + data)
  │     │     │    支持 Bitmap 模式下 pack 级跳过 (rs_pack_filter)
  │     │     │
  │     │     └─ DeltaValueSpace: DeltaMergeBlockInputStream
  │     │          读 ColumnFiles (磁盘) + MemTable (内存) 的增量行
  │     │          与 stable 的 block 按 (handle, version) 归并
  │     │          同 handle 时 delta 行覆盖 stable 行
  │     │
  │     ├─ DMRowKeyFilterBlockInputStream
  │     │     按 key range 过滤 (只保留 read_ranges 内的行)
  │     │
  │     └─ DMVersionFilterBlockInputStream<MVCC>
  │           逐行判断:
  │             version > start_ts  → 不可见, 跳过
  │             is_deleted           → 已删除, 跳过
  │             否则                 → 保留, 输出
  │
  └─ 4. 产出: Block (只含 columns_to_read 中的列)
```

| 步骤 | 工作内容 | 耗时分布 (以 lineitem 为例) |
|---|---|---|
| `tot_build_bitmap` | 构建 pack filter bitmap | 50ms |
| `tot_build_inputstream` | 创建 segment stream chain | 2455ms |
| `tot_rs_index_check` | pack 级 rough set 过滤 | 170ms |
| `tot_read` (aggregate) | 实际 DM file IO | 36500ms (agg, 72-way → ~500ms/stream) |
| MVCC 过滤 | DMVersionFilter 逐行过滤 | 在 stream read 内部, 不计入单独项 |

### Columnar: 单 ColumnarReader 读取

```
ffi_read_block() → ColumnarMvccReader
  │
  ├─ 1. ColumnarTableReader::read(block, limit=1024)
  │     │
  │     │  while block 未填满:
  │     │
  │     ├─ Step 1: version 列 load_pack()
  │     │     ColumnarColumnReader::load_pack(pack_idx)
  │     │       → packs_filter[pack_idx] == None? → 整 pack 跳过 (Late Materialization)
  │     │       → PackLoader::load_pack()
  │     │            read_from_segment_cache() → S3/disk 读压缩 pack
  │     │            decompress_pack()         → LZ4 解压
  │     │            col_buf.parse()           → 列格式解析
  │     │
  │     ├─ Step 2: handle 列 load_pack()  (同一 pack_idx)
  │     │     同上流程
  │     │
  │     └─ Step 3: 逐数据列 load_pack()  (同一 pack_idx)
  │           对 columns_readers[] 中的每一列:
  │             同上 load_pack 流程
  │
  ├─ 2. ColumnarMvccReader::try_read_block()   ← MVCC 后处理
  │      逐行扫 version + handle:
  │        version > read_ts          → skip (不可见)
  │        handle == prev_handle (dup) → skip
  │        version is null (deleted)   → skip
  │        handle >= end_handle        → break
  │        否则                        → start_range (可见)
  │      → 构建 ColumnarFilter (bitset)
  │      → 后续只在 visible 范围内迭代
  │
  ├─ 3. ffi_read_handle() → 返回 handle 列的数据 (TiFlash 侧用)
  │
  └─ 4. ffi_read_column(col_id) → 逐数据列返回
        将 parse 后的列数据序列化为 TiFlash 格式:
          serialize_for_tiflash → 写入返回 buffer
```

| 步骤 | 工作内容 | 火焰图占比 |
|---|---|---|
| `load_pack` | S3/disk 读 + LZ4 解压 + parse | 27.1% |
| `decompress_pack` + LZ4 | LZ4 解压 | 27.7% (15.9+11.7) |
| `ffi_read_block` (整体) | tokio block_on 等待异步完成 | 41.5% |
| `serialize_for_tiflash` | 列数据序列化为 TiFlash 格式 | 7.8% |
| MVCC 过滤 | 逐行扫 version + handle, 构建 bitset | 在 read_block 内部 |
| `pack_filter` 跳过 | 整 pack 标记为 None (Late Materialization) | 在 load_pack 内部 |

### 存储层级 & 多文件 merge 对比

两边都有多层存储结构和 merge 过程。Columnar 同样需要 merge，不是"纯快照读"。

```
TiFlashWrite 存储层级:
  DeltaValueSpace:
    ├─ MemTable (内存增量, SKL)         → DeltaMergeBlockInputStream
    └─ ColumnFile (磁盘增量, DM 行格式)   → (stable 与 delta 按 handle 归并)
  StableValueSpace:
    └─ DMFile (已压缩到 S3, 列式布局)    → DMFileBlockInputStream

  merge: DeltaMergeBlockInputStream 逐 block merge stable + delta
         按 (handle, version) 归并, delta 的同 handle 行覆盖 stable
  MVCC:  DMVersionFilterBlockInputStream (merge 后逐行过滤)

Columnar 存储层级:
  增量层 (行格式):
    ├─ MemTables (内存 SKL)             → RowMvccReader (行格式, 自带 MVCC)
    └─ Unconverted L0 (SSTable, 未转列存) → TableIterator (行格式读取)
  列存层:
    ├─ Columnar Level 1: ColumnarTableReader × N (已转列存的单文件)
    └─ Columnar Level 2: ColumnarConcatReader (多文件合并读取)

  全部 readers → ColumnarMergeReader (heap merge, 按 handle 归并)
               → ColumnarMvccReader (merge 后逐行 MVCC)
```

**两边都：有增量层 + 有 merge + merge 后 MVCC。** 差异在于 merge 和 IO 的方式。

### MVCC & 数据流对比

```
TiFlashWrite:
  Stable(DM file, 列式) ──┐
                           ├─ DeltaMergeBlockInputStream ──► DMVersionFilter ──► output
  Delta(ColumnFile, 行式) ─┘  (逐 block merge 归并行)         (逐行 MVCC)

  特点: merge-then-filter
  - stable (列式 block) 和 delta (行式 block) 逐 block 归并
  - merge 后 DMVersionFilter 逐行 MVCC
  - delta index cache 加速 version chain 查找
  - handle + version + data 在同一次 block read 中一起读出

Columnar:
  MemTable (行) ──┐
  Unconv L0 (行) ─┤
  Col L1 (列)   ──┼─ ColumnarMergeReader ──► ColumnarMvccReader ──► serialize ──► output
  Col L2 (列)   ──┘  (heap merge 归并行)      (逐行 bitset MVCC)     (转 TiFlash 格式)

  特点: 行列混合 merge-then-filter
  - 行格式 reader (RowMvccReader) 和列格式 reader (ColumnarTableReader) 混合
  - ColumnarMergeReader heap merge 归并行
  - merge 后 ColumnarMvccReader 逐行扫 version+handle 构建 bitset
  - version/handle/data 逐列独立 load_pack (S3/disk → LZ4 解压 → parse)
```

| 差异维度 | TiFlashWrite | Columnar |
|---|---|---|
| **增量层格式** | ColumnFile (DM 行格式) + MemTable (SKL) | Unconverted L0 (SSTable) + MemTable (SKL) |
| **稳定层格式** | DMFile (列式压缩) | Columnar Level 1/2 (列式压缩) |
| **行/列混合** | delta 行格式, stable 列格式，逐 block merge | 增量行格式 (RowMvccReader), 列存层列格式，heap merge |
| **merge 方式** | `DeltaMergeBlockInputStream` 逐 block 归并 | `ColumnarMergeReader` heap merge 逐 batch |
| **MVCC 方式** | `DMVersionFilter` merge 后逐行过滤 | `ColumnarMvccReader` merge 后逐行构建 bitset |
| **MVCC 索引** | `prepareMVCCIndex` delta index cache | 无专用 cache (逐行扫 version) |
| **列 IO 粒度** | handle+version+data 在同一 block 中一次性读出 | 逐列独立 load_pack (version→handle→col1→col2→...) |
| **Pack 跳过** | `DMFilePackFilterResults` (DM file 层) | `packs_filter` (Late Materialization) |
| **格式转换** | DM block 直接可用 | `serialize_for_tiflash` 逐列转换为 TiFlash 格式 |

## 单列数据存储与读取模型

### DMFile (TiFlashWrite) — "列连续" 布局

```
DMFile 目录结构:
  dmf_<id>/
    col_2.dat     ← 单列完整数据文件, 所有 pack 连续存储
    col_2.mrk     ← 索引: pack_idx → {offset_in_compressed_file, offset_in_decompressed_block}
    col_2.null.dat / col_2.size0.dat  ← nullable 列的子流文件

pack 在文件内的布局:
  [pack_0][pack_1][pack_2]...[pack_N]  ← 物理连续, 压缩块边界由 marks 索引描述
  ↑ 整列所有 pack 在单个文件中顺序排列

读取模型 (DMFileReader::readFromDisk):
  ColReadStream::buf->seek(offset_in_file, offset_in_decompressed_block)  ← 1 次定位
    → CompressedSeekableReaderBuffer 内部:
        seek 到 pack 起始偏移
        流式读取 + 解压后续的 N 个 pack
        (pack 之间无额外 seek — 压缩块边界由 marks 索引驱动)
  → deserializeBinaryBulkWithMultipleStreams(buf->get(), read_rows)  ← 反序列化

  pack_count > 1 时仍然只需 1 次 seek:
    buf->seek(pack_0_offset) → 顺序解压 pack_0, pack_1, ..., pack_{N-1}

实测 (lineitem 300M 行, 4 列):
  14,349 seeks / 4,783 blocks / 4 列 = 0.75 seeks/(列·block)
  → CompressedSeekableReaderBuffer 内部缓冲, 跨 readImpl 调用也能命中
```

### Columnar (ColumnFile) — "pack 独立" 布局

```
ColumnFile 目录结构:
  <file_id>_<col_id>    ← 每列的数据文件
    pack 在文件内独立存储, 不保证连续性
    每个 pack 有独立的偏移和长度 (由 ColumnMeta::pack_offsets 索引)

pack 布局:
  [pack_0]               ← 独立存储单元
  ... 可能存在间隙 ...
  [pack_1]               ← 下一个 pack
  ... 可能存在间隙 ...
  (pack 之间不保证物理连续)

读取模型 (PackLoader::load_pack → read_from_segment_cache):
  每个 pack 独立处理:
    file.read_at_async(pack_offset, pack_length)  ← 1 次 seek + read
    大 pack (>256KB):  直接 read_at_async, 不经过 segment cache
    小 pack (≤ 256KB):  segment cache 预读 256KB region
                        命中 → 从缓存拷贝, 0 seek
                        miss → read_at_async 读取 256KB segment, 1 seek
    decompress_pack(compressed_buf, uncompressed_buf)  ← 独立 LZ4 解压
    col_buf.parse(uncompressed_buf)                     ← 独立列格式解析

  每个 pack 都是独立 IO 单元, 互不共享

实测 (lineitem 300M 行, 4 列):
  330,837 seeks / ~4,658 blocks / 4 列 = 17.8 seeks/(列·block)
  → 每列每个 block 平均跨越 ~18 个 pack, 每个 pack 1 次独立 seek
```

### 差异图解

```
DMFile (列连续):
  buf->seek(pack_0_offset)
  ┌──────┬──────┬──────┬──────┬──────┐
  │pack_0│pack_1│pack_2│pack_3│pack_4│ → 顺序流式读出
  └──────┴──────┴──────┴──────┴──────┘
  1 次 seek, 顺序解压 N 个 pack

Columnar (pack 独立):
  load_pack(0)      load_pack(1)      load_pack(2)
  ┌──────┐           ┌──────┐           ┌──────┐
  │pack_0│           │pack_1│           │pack_2│
  └──────┘           └──────┘           └──────┘
  seek+read         seek+read         seek+read
  LZ4 解压          LZ4 解压          LZ4 解压
  col_buf.parse     col_buf.parse     col_buf.parse
```

### 差异根因

| | DMFile (TiFlashWrite) | Columnar (ColumnFile) |
|---|---|---|
| **存储布局** | 单列单文件, pack 连续 | pack 独立存储, 不保证连续 |
| **索引粒度** | `marks[]` 描述每个 pack 的偏移, 支持连续解压 | `pack_offsets` 给出每个 pack 起止, 每 pack 独立定位 |
| **读取方式** | 1 次 seek, 顺序流式解压后续 pack | 每 pack 1 次 seek + 独立解压 |
| **压缩方式** | DM file 整体压缩框架 (`CompressedSeekableReaderBuffer`) | 每 pack 独立 LZ4 压缩 |
| **read_ahead** | `CompressedSeekableReaderBuffer` 内部缓冲, 跨 readImpl 共享 | `segment_cache` (256KB), 仅缓存小 pack 的周边 region |
| **IO seeks/列/block** | 0.75 | 17.8 |
| **总 seeks** | **14,349** | **~330,837** |

### 可行性评估

Columnar 是否可以实现类似 DMFile 的 "列连续读取" 优化？

1. **pack 合并**：在 columnar compaction 时，将同一列的多个小 pack 合并为更大的连续块。但这影响写入路径。

2. **pack 预读**：在 `read_from_segment_cache` 中，当加载 pack 时，可将其后续 N 个 pack 也一并预读（增大 segment cache 或添加 pack read-ahead buffer）。这是纯读取侧优化。

3. **batch load_pack**：类似 DMFileReader 的 `readColumn` 一次读多个 pack (`pack_count > 1`)，可以将 `read_from_segment_cache` 的读取范围从单个 pack 扩展为连续多个 pack 的范围。

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
