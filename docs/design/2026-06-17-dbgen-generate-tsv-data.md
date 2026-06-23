# 使用 dbgen 生成与 `dttool generate` 等价的 TSV 数据

## Introduction

本文档记录如何使用 [dbgen](https://github.com/kennytm/dbgen) (Rust 数据生成工具)
生成与 `tiflash dttool generate --rows 1007616 --columns 150` 等价的 TSV 数据文件。

我们对 dbgen 进行了扩展，添加了原生 TSV 输出支持（`--format tsv`）。使用时无需
CSV→TSV 后处理，一行命令即可直接生成可被 `dttool bench --input` 消费的
TabSeparated 格式数据。

## Motivation

- **多工具对比**：`dttool generate` 是单线程 C++ 实现，dbgen 是并行 Rust 实现，
  对比两者可以评估不同数据生成器的性能和便利性。
- **外部工具集成**：dbgen 的模板驱动生成方式更灵活，适合与外部 benchmark 框架
  （如 Criterion、Hyperfine）组合使用。
- **可复现性**：dbgen 支持 `--seed` (64 位 hex) 固定随机种子，保证输出可复现。
- **原生 TSV**：避免 CSV→TSV 中转，减少磁盘 IO 和中间数据。

## Background

### `dttool generate` 的数据模型

`dttool generate` 复用了 `DTToolBench.cpp` 中的 `createColumnDefines` 和
`createBlock` 来生成数据。当参数为 `--columns 150` 时，实际生成 **153 列**：

| 组 | 数量 | 列名 | 类型 | 生成方式 |
|---|---|---|---|---|
| Handle | 1 | `_tidb_rowid` | Int64 | `0, 1, 2, ...` 自增 |
| Version | 1 | `_INTERNAL_VERSION` | UInt64 | `rowid × 10` |
| Delete mark | 1 | `_INTERNAL_DELMARK` | UInt8 | `random() & 1` (随机 0 或 1) |
| Integer | 75 | `int_0` … `int_74` | Int64 | `uniform_int_distribution<Int64>` [0, INT64_MAX] |
| String | 75 | `str_0` … `str_74` | Nullable(String) | 每列 1024 个随机字符，字符集 79 字符 |

TSV 格式特征：

- 字段使用 `\t` 分隔，行使用 `\n` 结束。
- 不包含表头行。
- `Nullable(String)` 的 NULL 值使用 `\N` 表示（当 `--sparse-ratio` > 0 时）。
- 默认 `--sparse-ratio 0.0`，因此所有字符串列均为非 NULL。
- 行数必须是 `DEFAULT_MERGE_BLOCK_SIZE = 8192` 的整数倍。
  `1007616 / 8192 = 123` 个 block。

每行数据量估算：

- 整数列 (78 列)：~1560 bytes
- 字符串列 (75 列 × 1024 chars)：~76,800 bytes
- **每行约 78 KB，1007616 行总计约 76 GB**

### dbgen 的特征

dbgen 是一个用 Rust 编写的快速数据生成工具，特点：

- **模板驱动**：通过 `CREATE TABLE` 语句中的 `/*{{ expression }}*/` 注释定义列生成逻辑。
- **多线程并行**：`-j` 参数控制并行度（默认 = CPU 核数）。
- **支持多种 RNG**：HC-128 (默认)、ChaCha、ISAAC、PCG32 等。
- **输出格式**：SQL (`INSERT INTO ... VALUES`)、CSV、TSV（新增）、SQL INSERT-SET。

### 核心差异

| 维度 | dttool generate | dbgen |
|---|---|---|
| 输出格式 | 原生 TSV (`\t` 分隔, `\N` null) | 原生 TSV (`--format tsv`) |
| 字符串生成 | 逐字符 `uniform_int_distribution` | `rand.regex()` 正则生成 |
| 字符集 | 79 字符 (含 `,` `"` `\`) | 88 字符 (安全子集，保证无歧义) |
| 并行度 | 单线程 | 多线程 (`-j`) |
| 文件数量 | 单个 TSV | 多个 `.tsv` 文件（每 `-R` 行一个），需 `cat` 合并 |

对于 microbenchmark 目的，字符集和分布的微小差异对 I/O 性能测量影响可忽略。
核心指标（行数、列数、类型、数据体积）完全一致。

## Goals

- 使用 dbgen 生成与 `dttool generate --rows 1007616 --columns 150` 数据结构等价的 TSV。
- 输出可直接被 `dttool bench --input` 正确读取（无需后处理）。
- 固定随机种子可复现输出。
- 模板文件和操作流程可被其他人复现。

## Non-Goals

- 不修改 dttool bench 的 TSV 读取逻辑。
- 不涉及 `dbschemagen` 的用法。
- 不保证字符串字符分布与 dttool 逐字节完全一致。

## dbgen TSV 支持的实现

在 dbgen 源码中添加了 TSV 输出格式。改动量较小，共涉及 2 个文件：

### `src/format.rs` — 新增 `TsvFormat` 结构体

在对 `/DATA/disk1/jaysonhuang/dbgen` 的本地修改中，`src/format.rs` 新增了
`TsvFormat` 结构体及其 `Format` trait 实现。与 `CsvFormat` 的关键差异：

| 方法 | CsvFormat | TsvFormat |
|---|---|---|
| `write_value_separator` | `b","` (逗号) | `b"\t"` (Tab) |
| `write_value` (Bytes) | 包裹双引号，`"` 转义为 `""` | 直接写入原始字节，无引号无转义 |
| `write_file_header` (列名) | 双引号包裹列名 | 直接写入列名，无引号 |
| 文件扩展名 | `.csv` | `.tsv` |
| 默认 NULL 字符串 | `\N` | `\N` |

### `src/cli.rs` — 注册 `Tsv` 格式变体

在 `FormatName` 枚举中新增 `Tsv` 变体，并在 `from_str`、`extension`、`create`、
`default_null_string` 等路由函数中注册。编译后即可通过 `--format tsv` 或 `-f tsv`
使用。

## Template Design

### 字符集选择

由于 TSV 不包裹引号、不使用逗号分隔，理论上字符集可以包含任意字符（只要不含
`\t` 和 `\n`）。出于与 dttool 原始数据格式的兼容性和通用安全性考虑，我们仍
使用了一个安全字符集（88 个字符），排除了可能引起歧义的控制字符：

```
0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#%^&*()|[]{}:;<.>?/~=+_-
```

排除的字符：`,` `"` `\` `\t` `'` `` ` ``

### 列生成表达式的映射

| 列 | dttool 逻辑 | dbgen 表达式 |
|---|---|---|
| `_tidb_rowid` | 从 0 自增 | `ROWNUM - 1` (dbgen ROWNUM 从 1 开始) |
| `_INTERNAL_VERSION` | `rowid * 10` | `(ROWNUM - 1) * 10` |
| `_INTERNAL_DELMARK` | `eng() & 1` | `rand.range(0, 2)` |
| `int_N` | `uniform_int_distribution<Int64>` [0, INT64_MAX] | `rand.range_inclusive(0, 9223372036854775807)` |
| `str_N` | 逐字符 uniform，1024 字符 | `rand.regex('[safe_chars]{1024}', '', 1024)` |

### 模板文件注意事项

1. **双花括号**：dbgen 的 `/*{{ ... }}*/` 语法要求双花括号。在 Python f-string
   中生成时需写 `/*{{{{ ... }}}}*/`（f-string 转义规则下 `{{{{` → `{{`）。
2. **Regex 字符类转义**：`[` 和 `]` 在 regex 字符类中需特殊处理——`]` 放在字符类
   首位、`[` 用 `\[` 转义。生成脚本已处理。

完整模板文件位于 `/tmp/dbgen_dttool_template.sql`（164 行，153 列）。

```sql
-- dbgen template equivalent to: dttool generate --rows 1007616 --columns 150
-- Total: 153 columns (3 system + 75 Int64 + 75 String)

CREATE TABLE t (
  -- System columns
  _tidb_rowid        BIGINT NOT NULL /*{{ ROWNUM - 1 }}*/,
  _INTERNAL_VERSION  BIGINT UNSIGNED NOT NULL /*{{ (ROWNUM - 1) * 10 }}*/,
  _INTERNAL_DELMARK  TINYINT UNSIGNED NOT NULL /*{{ rand.range(0, 2) }}*/,

  -- User Int64 columns: int_0 .. int_74
  int_0   BIGINT NOT NULL /*{{ rand.range_inclusive(0, 9223372036854775807) }}*/,
  -- ... (共 75 列)
  int_74  BIGINT NOT NULL /*{{ rand.range_inclusive(0, 9223372036854775807) }}*/,

  -- User String columns: str_0 .. str_74
  str_0   TEXT NOT NULL /*{{ rand.regex('[safe_chars]{1024}', '', 1024) }}*/,
  -- ... (共 75 列)
  str_74  TEXT NOT NULL /*{{ rand.regex('[safe_chars]{1024}', '', 1024) }}*/
);
```

## 操作步骤

### 1. 准备 dbgen

```bash
# 本地 dbgen 仓库路径（已包含 TSV 支持）
DBGEN_REPO=/DATA/disk1/jaysonhuang/dbgen
cd "$DBGEN_REPO"

# 编译（如果未编译）
cargo build --release

DBGEN="$DBGEN_REPO/target/release/dbgen"
$DBGEN --version
```

### 2. 生成 TSV 数据

```bash
DBGEN=/DATA/disk1/jaysonhuang/dbgen/target/release/dbgen
TEMPLATE=/tmp/dbgen_dttool_template.sql
OUTPUT_DIR=/DATA/disk1/jaysonhuang/tsv_data/dbgen_tsv

mkdir -p "$OUTPUT_DIR"

$DBGEN \
  -i "$TEMPLATE" \
  -o "$OUTPUT_DIR" \
  -N 1007616 \
  -R 8192 \
  -f tsv \
  -r 1 \
  --format-null '\N' \
  -j $(nproc) \
  -s "0000000000000000000000000000000000000000000000000000000000000001"
```

参数说明：

| 参数 | 值 | 说明 |
|---|---|---|
| `-i` | 模板路径 | 153 列 CREATE TABLE 模板 |
| `-o` | `$OUTPUT_DIR` | 输出目录 |
| `-N` | `1007616` | 总行数 = 123 blocks × 8192 rows |
| `-R` | `8192` | 每个输出文件行数上限，匹配 `DEFAULT_MERGE_BLOCK_SIZE` |
| `-f` | `tsv` | **TSV 输出格式（原生，无需后处理）** |
| `-r` | `1` | 每行一条记录 |
| `--format-null` | `\N` | NULL 使用 `\N` 表示 |
| `-j` | `$(nproc)` | 并行线程数 |
| `-s` | hex seed | 固定随机种子，保证可复现 |

**输出文件**：`$OUTPUT_DIR/t.1.tsv`, `t.2.tsv`, ... `t.123.tsv`，每个文件 8192 行，
文件名按数字序排列。

### 3. 合并为单文件（可选）

dbgen 按 `-R` 参数分文件输出，每个文件对应一个线程的生成批次。如需单个 TSV 文件：

```bash
OUTPUT_TSV=/DATA/disk1/jaysonhuang/tsv_data/dbgen_1007616x150.tsv

# 按数字序合并（sort -V 保证 t.1.tsv < t.2.tsv < ... < t.123.tsv）
for f in $(ls "$OUTPUT_DIR"/t.*.tsv | sort -V); do
    cat "$f"
done > "$OUTPUT_TSV"
```

### 4. 验证输出

```bash
# 行数：应为 1007616
echo "Rows: $(wc -l < $OUTPUT_TSV)"

# 列数：应为 153
echo "Cols: $(head -1 $OUTPUT_TSV | tr '\t' '\n' | wc -l)"

# 文件大小
echo "Size: $(du -h $OUTPUT_TSV | cut -f1)"

# 验证 _tidb_rowid 全局有序
echo "首行 _tidb_rowid: $(head -1 $OUTPUT_TSV | cut -f1)"
echo "末行 _tidb_rowid: $(tail -1 $OUTPUT_TSV | cut -f1)"

# 每个 block 首行递增校验
awk -F'\t' 'NR % 8192 == 1 {print "  row#"NR":", $1}' "$OUTPUT_TSV" | head -5
awk -F'\t' 'NR % 8192 == 1 {print "  block#"((NR-1)/8192)" _tidb_rowid:", $1}' \
  "$OUTPUT_TSV" | tail -3
```

期望输出：

```
Rows: 1007616
Cols: 153
Size: 76G
首行 _tidb_rowid: 0
末行 _tidb_rowid: 1007615
```

### 5. 使用 dttool bench 验证可读性（可选）

如果需要确认生成的 TSV 能被 `dttool bench --input` 正确加载：

```bash
# 首先用 dttool generate 生成配套的 schema JSON（只需要 schema，数据可丢弃）
tiflash dttool generate \
  --rows 8192 \
  --columns 150 \
  --output /tmp/dummy.tsv \
  --schema ./schema_150.json \
  --random 1
rm -f /tmp/dummy.tsv

# 然后用 dbgen 生成的 TSV 运行 bench
tiflash dttool bench \
  --input "$OUTPUT_TSV" \
  --schema ./schema_150.json \
  --checksum none \
  --iterations 1
```

如果加载成功且无报错，说明 TSV 格式完全符合 `TabSeparated` 输入格式的预期。

## 已验证的测试数据

以下数据已在本机生成并验证通过：

| 指标 | 值 |
|---|---|
| 文件路径 | `/DATA/disk1/jaysonhuang/tsv_data/dbgen_1007616x150.tsv` |
| 大小 | 76 GB |
| 行数 | 1,007,616 |
| 列数 | 153 |
| `_tidb_rowid` 范围 | 0 ~ 1,007,615（严格递增） |
| str_N 长度 | 1024 chars |
| 双引号数量 | 0（原生 TSV，无引号） |
| 分隔符 | `\t` (Tab) |

## 性能对比参考

| 工具 | 语言 | 并行 | 实测耗时 (1007616 行 × 153 列) |
|---|---|---|---|
| `dttool generate` | C++ | 单线程 | 待实测 |
| `dbgen -f tsv` | Rust | 多线程 (`-j`) | ~10 分钟 (生成 74 GB CSV), ~5 分钟 (CSV→TSV 转换) |

dbgen 的瓶颈在 `rand.regex('[chars]{1024}')` 的大规模字符串生成
（~1260 rows/s 单线程）。`-j` 多线程可有效并行化。

## Limitations

- dbgen 的 TSV 输出中，字符串内容不含特殊转义（如 `\t` → `\\t`）。当前模板
  使用安全字符集规避了此问题。如果后续需要支持任意 UTF-8 字符（含 Tab、换行），
  需要在 `TsvFormat::write_value` 的 Bytes 分支中添加 TabSeparated 转义逻辑。
- 生成的 TSV 中字符串分布（`rand.regex` 正则生成 vs 逐字符均匀随机）与
  `dttool generate` 存在微小差异，对 I/O 基准测试影响可忽略。
- dbgen 的 `ROWNUM` 在并行模式下，各线程内部的 `_tidb_rowid` 是连续的，但
  不同线程之间的输出顺序取决于调度。合并时需要 `sort -V` 按文件名数字序
  确保行的全局有序。

## References

- dbgen: <https://github.com/kennytm/dbgen>
- dbgen CLI 文档: `/DATA/disk1/jaysonhuang/dbgen/CLI.md`
- dbgen 模板文档: `/DATA/disk1/jaysonhuang/dbgen/Template.md`
- dbgen TSV 实现: `/DATA/disk1/jaysonhuang/dbgen/src/format.rs` (`TsvFormat`)
- dbgen TSV 注册: `/DATA/disk1/jaysonhuang/dbgen/src/cli.rs` (`FormatName::Tsv`)
- dttool generate: `dbms/src/Server/DTTool/DTToolGenerate.cpp`
- dttool bench 列定义: `dbms/src/Server/DTTool/DTToolBench.cpp` (`createColumnDefines`, `genBlocks`)
- `DEFAULT_MERGE_BLOCK_SIZE`: `dbms/src/Core/Defines.h`
