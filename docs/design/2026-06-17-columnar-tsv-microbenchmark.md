# Columnar TSV Microbenchmark 需求文档

## 摘要

需要为 `contrib/cloud-storage-engine/components/kvengine` 增加一个
Columnar 文件层的 microbenchmark。该 benchmark 读取
`tiflash dttool generate` 生成的 TSV 数据文件和 schema JSON 文件，将其转换为
kvengine Columnar 写入路径使用的 `Schema` 和 `Block`，再通过
`ColumnarFileBuilder` / `ColumnarTableBuilder` 生成 Columnar 文件，并通过
`ColumnarFile` / `ColumnarTableReader` 扫描读取。

该需求的目标是复用固定的 TSV 数据集衡量 Columnar 文件格式的写入和读取成本。
读取 benchmark 默认从本地磁盘上的 Columnar 文件读取，但不覆盖远端 DFS IO、
Columnar Hub FFI 或完整 TiFlash 查询链路。

## 背景

### 当前状态

`tiflash dttool generate` 已经可以生成用于性能测试的 TSV 数据文件和 schema
JSON 文件。输出特征包括：

- TSV 不包含表头。
- 字段使用 `\t` 分隔，行使用 `\n` 结束。
- `Nullable(String)` 的 NULL 值使用 `\N` 表示。
- schema JSON 只包含列名和类型名，不包含 column id、table id、row count、
  random seed 或生成参数。
- 生成行数要求是 `8192` 的整数倍。

生成的数据模型沿用 `DTToolBench.cpp` 的列定义：

- 前三列为隐藏列：handle、version、delete mark。
- 后续用户列包括 `Int64` 整数列和 `Nullable(String)` 字符串列。

Columnar major compaction 的输出由 `contrib/cloud-storage-engine` 中的
kvengine 负责：

- change set 中的持久化元数据是 `kvenginepb::ColumnarCreate`。
- 文件构建路径使用 `ColumnarFileBuilder` 和 `ColumnarTableBuilder`。
- 打开后的 Columnar 文件对象是 `ColumnarFile`。
- 表级读取使用 `ColumnarTableReader`，它实现 `ColumnarReader`。
- major compaction 的核心写入流程是读取 `Block`，调用
  `ColumnarTableBuilder::append_block`，再将 table builder 加入
  `ColumnarFileBuilder`。

`contrib/tiflash-columnar-hub` 负责通过 FFI 将 TiFlash C++ 侧请求桥接到
`CloudColumnarReaders`。它适合端到端读取链路测试，但不是 Columnar 文件格式和
文件 builder 的所有者。

### 问题陈述

当前如果要压测 kvengine Columnar 文件写入和读取，需要在 Rust 侧构造数据或通过
现有测试辅助函数间接生成数据。这样难以与 `dttool generate` 生成的固定测试集对齐，
也难以复用同一份输入对比 DMFile 和 Columnar 文件格式。

需要新增一个明确的 TSV 输入 benchmark 流程，让 Columnar 文件写入和读取可以消费
`dttool generate` 的输出，并将数据准备成本从实际测量区间中剥离。

### 约束

- 初始输入格式只支持 `dttool generate` 当前输出的 TSV 和 schema JSON。
- schema JSON 只有列名和类型名，因此 benchmark 必须根据 `dttool generate`
  的列顺序约定派生 kvengine 所需的 table id、column id、handle column 和
  version column。
- kvengine 中部分 Columnar 低层 API 目前是 `pub(crate)`，例如
  `ColumnarTableBuilder::append_block`、`ColumnBuffer::push_*` 和
  `ColumnarTableReader`。普通 Rust `benches/` 目标不能直接访问这些接口。
- benchmark 不应扩大生产路径公开 API 的暴露面。若需要暴露辅助入口，应使用
  `testexport` feature 或放在 crate 内部的 bench/test helper 中。
- TSV 解析不应计入 Columnar 写入或读取 benchmark 的核心耗时。
- 初始版本可测本地磁盘文件读取，但不测远端对象存储、DFS 上传下载、
  Columnar Hub FFI 和 TiFlash 查询执行。

## 目标

1. 支持使用 `dttool generate` 的 TSV 和 schema JSON 作为 Columnar benchmark 输入。
2. 构建与 Columnar major compaction 输出一致的 Columnar 文件结构。
3. 分别测量 Columnar 文件构建成本和 Columnar 文件扫描读取成本。
4. 明确区分数据准备、文件构建、文件打开和文件读取的耗时边界。
5. 支持本地磁盘文件写入和本地磁盘文件读取，并将本地文件 IO 与 Columnar 编解码成本
   区分开。
6. 支持当前 generator 输出的类型集合：handle、version、delete mark、`Int64`
   和 `Nullable(String)`。
7. 保持 benchmark 可重复运行，并能通过固定 TSV 数据集复现结果。
8. 避免为了 benchmark 直接放宽生产 API 可见性。

## 非目标

- 不支持 CSV。
- 不支持 TSV 表头。
- 不支持任意 TiDB 表 schema。
- 不支持 arbitrary JSON schema，只支持 `dttool generate` 兼容 schema。
- 不修改 Columnar major compaction 的生产行为。
- 不修改 `tiflash dttool generate` 的输出格式。
- 不把 `tiflash-columnar-hub` FFI 读路径纳入初始 microbenchmark。
- 不测远端 DFS 或对象存储 IO。
- 不把 TSV 解析成本混入 Columnar 文件写入和读取指标。

## 术语

| 术语 | 含义 |
| --- | --- |
| TSV input | `tiflash dttool generate` 生成的无表头 TSV 数据文件。 |
| schema JSON | `tiflash dttool generate` 生成的列名和类型描述文件。 |
| Columnar file | kvengine Columnar 文件格式，打开后对应 `ColumnarFile`。 |
| Columnar create | change set 中记录 Columnar 文件创建结果的 `ColumnarCreate` 元数据。 |
| Bench block | 从 TSV 转换得到的 `kvengine::table::columnar::Block`。 |
| Timed section | Criterion 或其他 benchmark 框架实际计时的代码区间。 |

## 需求设计

### 1. 代码归属

初始实现应放在 `contrib/cloud-storage-engine/components/kvengine` 中，而不是放在
`contrib/tiflash-columnar-hub` 中。

推荐结构：

```text
contrib/cloud-storage-engine/components/kvengine/
  benches/
    columnar_tsv.rs
  src/table/columnar/
    bench_util.rs     # cfg(any(test, feature = "testexport"))
```

`benches/columnar_tsv.rs` 负责 benchmark 配置、数据预加载、Criterion 分组和指标输出。
`bench_util.rs` 负责访问 crate 内部 Columnar API，包含 TSV 到 `Block` 的转换、
Columnar 文件构建和 Columnar 文件读取辅助函数。

`bench_util.rs` 应使用 `#[cfg(any(test, feature = "testexport"))]` 保护，避免成为
生产 API。

### 2. 输入接口

benchmark 必须能指定以下输入：

| 配置 | 说明 |
| --- | --- |
| TSV path | `dttool generate` 输出的数据文件路径。 |
| Schema path | `dttool generate` 输出的 schema JSON 文件路径。 |
| Table ID | 构建 kvengine `Schema` 时使用的 table id，默认可为 `1`。 |
| Columnar level | 传给 `ColumnarTableBuilder::new` 的 level，默认使用 `2` 以模拟底层 major compaction 输出。 |
| Workdir | 写入本地 Columnar 文件的目录。 |
| Read limit | 每次 `ColumnarReader::read` 的行数限制，默认 `8192`。 |
| Sync local output | 是否在本地文件写入后调用 `sync_all`，默认关闭。 |

为了避免引入新的命令行依赖，初始版本可以使用环境变量提供路径和参数，例如：

```text
COLUMNAR_TSV_INPUT=/tmp/data.tsv
COLUMNAR_SCHEMA_INPUT=/tmp/schema.json
COLUMNAR_TABLE_ID=1
COLUMNAR_LEVEL=2
COLUMNAR_WORKDIR=/tmp/columnar-tsv-bench
COLUMNAR_READ_LIMIT=8192
COLUMNAR_SYNC_LOCAL_OUTPUT=0
```

运行方式示例：

```bash
cargo bench -p kvengine --features testexport --bench columnar_tsv
```

如果未设置 TSV 或 schema 路径，benchmark 应明确跳过 TSV 用例或返回可读错误，
不能静默生成另一份随机数据。

### 3. Schema 映射规则

schema JSON 只包含列名和类型名，因此 benchmark 应只接受与 `dttool generate`
兼容的列布局。

映射规则：

1. TSV/schema 的前三列按顺序解释为 handle、version、delete mark。
2. handle 列映射为 kvengine int handle column，使用
   `new_int_handle_column_info()`。
3. version 列映射为 kvengine version column，使用
   `new_version_column_info()`。
4. delete mark 不作为普通用户列写入 Columnar 文件，而是映射到 version column
   的 delete 标记，即 `push_version(version, is_delete)` 中的 `is_delete`。
5. 后续 `Int64` 用户列映射为 `tipb::ColumnInfo` 的 `LongLong` 类型。
6. 后续 `Nullable(String)` 用户列映射为可空字符串类型，例如 `VarChar` 或与现有
   Columnar 读取序列化兼容的字符串类型。
7. 用户列 column id 按 `dttool generate` 约定派生，起始 id 为 `3`，并保持 TSV
   字段顺序。

不符合这些规则的 schema 应在数据加载阶段失败，并给出明确错误。

### 4. TSV 到 Block 的转换

TSV 解析阶段应发生在 timed section 之前。

转换要求：

- 每个 TSV row 的字段数必须等于 schema JSON 中的列数。
- handle 解析为 `i64`，写入 little-endian 8 字节。
- version 解析为 `u64`。
- delete mark 解析为 `UInt8`，`0` 表示非删除，非 `0` 表示删除。
- `Int64` 用户列解析为 `i64`，写入 little-endian 8 字节。
- `Nullable(String)` 用户列遇到 `\N` 时写入 NULL；非 NULL 值按 TSV escaped
  规则还原为 bytes 后写入。
- 加载完成后的总行数必须是 `8192` 的整数倍。
- `Block` 内部行顺序应保持与 TSV 一致。输入数据通常已经按 handle 递增生成，
  benchmark 不应在加载阶段重新排序。

由于 `Block` 的字段和 `ColumnBuffer::push_*` 当前是 crate 内部接口，转换逻辑应放在
kvengine crate 内部的 helper 中，不应在外部 bench 目标里复制私有结构。

### 5. Columnar 文件写入 benchmark

写入 benchmark 应拆成两个 case，分别衡量 Columnar 编码构建成本和本地文件落盘成本。
这样可以回答两个不同问题：

1. `build_columnar_file`：只测 `ColumnarFileBuilder` / `ColumnarTableBuilder` 将
   `Block` 编码成 Columnar bytes 的成本。
2. `write_columnar_local_file`：测 Columnar bytes 写入本地磁盘文件的成本。该 case
   可以使用预先构建好的 bytes，避免把编码成本重复计入本地文件 IO。

如果需要端到端本地写入指标，可以额外增加 `build_and_write_columnar_local_file`，
其 timed section 同时包含 Columnar 构建和本地文件写入。该 case 的名称必须明确
包含 `build_and_write`，避免与纯构建或纯本地写入混淆。

`build_columnar_file` 的建议流程：


```text
preload: TSV + schema -> Vec<Block> / prepared rows

timed:
  ColumnarFileBuilder::new(file_id, snap_version, None)
  ColumnarTableBuilder::new(schema, opts, None, file_id, level)
  for block in prepared_blocks:
      append_block(block)
  file_builder.add_table(table_builder)
  file_builder.build()
```

`write_columnar_local_file` 的建议流程：

```text
preload:
  TSV + schema -> Vec<Block> / prepared rows
  build columnar bytes once

timed:
  create/truncate local output file under workdir
  write all columnar bytes
  optionally sync_all when COLUMNAR_SYNC_LOCAL_OUTPUT=1
  black_box(file_size)
```

要求：

- 默认 `ColumnarTableBuildOptions` 应与生产默认值保持一致：
  - `pack_max_row_count = 8192`
  - `pack_max_size = 256 KiB`
  - `max_columnar_table_size = 32 MiB`
- 默认 `level = 2`，以包含 bottom level min-max 构建路径。
- 如果 `level = 2`，`snap_version` 可以为 `None`；如果后续支持 level 1 测试，可使用
  `Some(snap_version)`。
- 每次 benchmark iteration 应重新构建 builder，避免复用已经消费过的 builder 状态。
- 输出 bytes 应通过 `black_box` 保留，避免被优化掉。
- `write_columnar_local_file` 每次 iteration 应使用唯一文件名或先 truncate 文件，
  避免追加写入影响结果。
- 默认不调用 `sync_all`，此时指标主要反映写入 page cache 的成本；启用
  `COLUMNAR_SYNC_LOCAL_OUTPUT=1` 后，指标包含更接近持久化的本地同步成本。

### 6. Columnar 文件读取 benchmark

读取 benchmark 默认应从本地磁盘文件读取，而不是使用 `InMemFile`。构建阶段产出的
Columnar bytes 应先写入 benchmark workdir 下的本地文件，然后通过 `LocalFile::open`
构造 `Arc<dyn File>`，再传给 `ColumnarFile::open`。

读取 benchmark 的 timed section 应拆分打开成本和扫描读取成本，避免把文件生成或
TSV 解析成本混入读取指标。

建议至少提供两个 case：

1. `open_columnar_file`：测量 `ColumnarFile::open` 解析 footer、properties 和 table
   offsets 的成本。
2. `scan_columnar_file`：预先打开本地 `ColumnarFile`，构造 reader，然后循环读取所有 rows。

扫描流程：

```text
preload:
  build columnar bytes
  write bytes to local columnar file
  LocalFile::open(file_id, local_path)
  ColumnarFile::open(Arc<LocalFile>, ...)

timed:
  ColumnarTableReader::new(...)
  reader.seek(empty_handle)
  while read_rows > 0:
      block.reset()
      reader.read(&mut block, read_limit)
```

本地磁盘读取会受到 OS page cache、文件系统和磁盘设备影响。benchmark 输出必须标注
读取来源是 `LocalFile`，并区分以下两类语义：

- warm-cache scan：文件已被打开或读过，主要反映 page cache 命中下的
  decode/decompress/scan 成本。
- cold-cache scan：在运行前尽量降低 page cache 影响，主要反映本地磁盘读取加
  decode/decompress/scan 的成本。

初始版本可以只实现 warm-cache scan。若实现 cold-cache scan，必须明确说明清 cache
方式和平台限制；不能依赖需要 root 权限的全局 drop cache 作为默认路径。

### 7. 指标输出

benchmark 应至少输出或记录以下上下文：

- TSV 文件路径。
- schema 文件路径。
- 加载总行数。
- 用户列数量。
- 字符串列数量。
- NULL 数量或 NULL 比例。
- 构建出的 Columnar 文件大小。
- Columnar level。
- 本地写入 workdir。
- 本地写入是否启用 `sync_all`。
- read limit。
- 每秒处理行数或每行耗时。

若使用 Criterion，可将 case name 包含关键参数，例如：

```text
columnar_tsv/build/l2/rows_1048576/cols_16
columnar_tsv/write_local/l2/rows_1048576/cols_16/sync_0
columnar_tsv/open/l2/rows_1048576/cols_16
columnar_tsv/scan/l2/rows_1048576/cols_16/read_8192
```

## 兼容性和不变量

- 输入 TSV 和 schema JSON 的契约必须与 `tiflash dttool generate` 保持一致。
- `\N` 只在 nullable 列中表示 NULL；非 nullable 列出现 `\N` 应报错。
- delete mark 必须转换为 version column 的 delete/null bitmap 语义，不能作为普通列写入。
- benchmark 不能改变生产 Columnar 文件格式。
- benchmark helper 不能无条件暴露到生产 API。
- 默认 pack 行数应保持 `8192`，与 `dttool generate` 的行数约束对齐。
- 本地文件写入和读取指标必须在 case name 或输出上下文中标明是否包含 `sync_all`
  或是否是 warm-cache / cold-cache。

## 验证策略

### 单元测试

应在 kvengine crate 内添加针对 helper 的测试：

- schema JSON 缺少 `columns`、`name` 或 `type` 时失败。
- TSV 字段数与 schema 列数不一致时失败。
- 行数不是 `8192` 的整数倍时失败。
- `Nullable(String)` 中的 `\N` 被转换为 NULL。
- 非 nullable 列出现 `\N` 时失败。
- delete mark 被转换为 version column 的 delete 标记。
- `Int64` 和 handle 的 little-endian 编码可被 reader 读回。

### Round-trip 测试

使用小型 TSV fixture：

1. TSV/schema -> `Block`。
2. `Block` -> `ColumnarFileBuilder` -> bytes。
3. bytes -> `ColumnarFile::open` -> `ColumnarTableReader`。
4. 读取结果与原始 `Block` 的 handle、version、delete mark 和用户列一致。

### Benchmark smoke test

提供一个小数据集或临时生成数据，验证：

- benchmark 在 `--features testexport` 下可以编译。
- 未设置 TSV/schema 路径时不会误跑随机数据。
- 设置 TSV/schema 路径后可以完成 build/write/open/scan 四类 case。

## 风险和缓解

1. schema JSON 信息不足，无法表达完整 TiDB schema。
   - 缓解：只支持 `dttool generate` 兼容 schema，根据列顺序派生 Columnar schema，
     其他输入直接失败。

2. benchmark 为了访问内部 API 过度扩大生产接口。
   - 缓解：新增 `cfg(any(test, feature = "testexport"))` helper，外部 bench 只依赖
     `testexport` feature，不直接公开 `ColumnBuffer` 写入 API。

3. benchmark 误把 TSV 解析成本计入 Columnar 写入读取成本。
   - 缓解：TSV/schema 加载必须发生在 Criterion timed section 之前，并在文档和 case
     name 中区分 preload 与 timed 部分。

4. 使用本地 `LocalFile` 读取时，结果会受到 OS page cache 和本地磁盘状态影响。
   - 缓解：benchmark 名称和输出说明标注为 local-file warm-cache 或 cold-cache；
     初始默认使用 warm-cache，并记录文件大小、读取来源和 read limit。

5. 使用本地文件写入时，默认不 `sync_all` 会更多测到 page cache 写入成本，而不是
   完整持久化成本。
   - 缓解：提供 `COLUMNAR_SYNC_LOCAL_OUTPUT` 或等价配置；case name 中标明
     `sync_0` / `sync_1`，并在结果说明中解释语义。

6. 大 TSV 数据集加载占用过多内存。
   - 缓解：初始实现可以按 8192 行构建 blocks，并在输出中记录加载后内存相关上下文；
     如后续需要更大数据集，再增加 streaming builder case。

## 分阶段计划

### 阶段一：内部 helper

- 增加 `table::columnar::bench_util`，使用 `testexport` feature 暴露给 bench。
- 实现 schema JSON 解析、TSV 解析、`Schema` 构建和 `Block` 构建。
- 增加 round-trip 单元测试。

### 阶段二：Criterion benchmark

- 增加 `benches/columnar_tsv.rs`。
- 实现 build/write/open/scan 四类 case。
- 输出数据规模、Columnar 文件大小、level、read limit 等上下文。

### 阶段三：扩展项（先不实现）

- 可选支持 `level = 1` 与 `level = 2` 对比。
- 可选增加 cold-cache 本地文件读取 case。
- 可选增加 Columnar Hub / `CloudColumnarReaders` 端到端读取 benchmark，但应作为独立文档或独立 benchmark 分组。

## 验收标准

- 可以使用 `dttool generate` 生成的 TSV/schema 运行 kvengine Columnar benchmark。
- benchmark 不调用随机数据生成作为 TSV 输入的替代。
- build/write/open/scan 指标边界清晰，TSV 解析不在 timed section 中。
- 写入 benchmark 包含本地文件落盘 case，并明确标注是否调用 `sync_all`。
- 读取结果能通过 round-trip 测试验证。
- 不需要无条件公开 Columnar 生产内部 API。
