# DATETIME、TIMESTAMP 和 DATE 类型的 Trim Min-Max 索引设计

- 状态：Draft
- 日期：2026-07-14

## 摘要

本文建议为 TiFlash DeltaMerge 存储中的 `DATETIME`、`TIMESTAMP` 和 `DATE` 列增加一种可选的 pack 级索引 `trim_minmax`。该索引只统计预定义“有效日期区间”内的非 NULL、非删除值，用于避免少量极端哨兵时间污染普通 min-max。例如，业务使用 `2100-01-01 00:00:00` 表示“尚未结算”时，只要一个 8192 行的 pack 中出现该值，普通 min-max 的 `max` 就会被抬高到 2100，进而削弱对近期短时间范围查询的 Rough Set Filter（RS Filter）能力。

第一版采用以下核心决策：

1. 有效日期区间定义为半开区间 `[1900-01-01 00:00:00, 2100-01-01 00:00:00)`。
2. `trim_minmax` 保存区间内值的 min/max；普通 min-max 保持不变，继续服务于不满足 trim 使用条件的查询。
3. 每个 trim 索引实际使用的上下界、格式版本、pack 数量和索引位置持久化在 DMFileMetaV2 的独立元数据块中；per-pack low/high flags 合并到 trim index 已有的 `has_null_marks` 字节数组中，磁盘语义统一为 `pack_marks`。
4. Reader 只能依据 DMFile 中保存的实际区间选择 trim 索引，不能依据当前版本的全局默认区间解释历史索引。
5. 只有能证明区间外低端值和高端值对谓词分别具有一致的匹配结果时，才能用 trim 索引替代该列的普通 min-max；第一版支持匹配集合位于有效区间内的 equality、IN、bounded range，以及有限边界位于有效区间内的单边 range。
6. Reader 根据 `has_trimmed_low` 和 `has_trimmed_high` 修正 trim rough check：区间外存在匹配值时不能保留 `None`，存在不匹配值时不能保留 `All`。
7. 代码复用 `MinMaxIndex` payload、serializer 和原始 rough check，通过组合式 trim wrapper 修正结果，不让 `TrimMinMaxIndex` 继承 `MinMaxIndex`。
8. 第一版仅为 DMFile V3 / MetaV2 写入 trim 索引；旧 DMFile 或不支持的谓词始终回退普通 min-max。

该设计不改变 SQL 语义，不要求 DDL，也不重写历史 DMFile。新旧 DMFile 可以同时存在，并在同一个查询中按文件独立选择 trim 或普通 min-max。

## 背景

### 当前普通 min-max 的生成与使用

`DMFileWriter` 当前会为 handle、整数和日期时间类型生成普通 min-max：

```text
DMFileWriter::write
  -> DMFileWriter::writeColumn
     -> MinMaxIndex::addPack
        -> 计算该列在当前 pack 内的 min/max
```

相关实现：

- `dbms/src/Storages/DeltaMerge/File/DMFileWriter.cpp`
- `dbms/src/Storages/DeltaMerge/Index/MinMaxIndex.cpp`

`MinMaxIndex::addPack` 会忽略 delete mark 对应的值，并从 v6.4.0 开始把 NULL 排除在 min/max 计算之外，同时独立保存 `has_null_marks` 和 `has_value_marks`。每个 pack 的普通索引逻辑上包含：

```text
has_null
has_value
min
max
```

查询侧由 `FilterParser::parseDAGQuery` 把 TiDB DAG 谓词转换成 `RSOperator`。`DMFilePackFilter` 按谓词涉及的列加载 min-max，并调用 `roughCheck` 生成每个 pack 的 `RSResult`：

```text
None     -> 不读取 pack
Some     -> 读取 pack，并执行行级过滤
All      -> 读取 pack，但可以跳过行级过滤
*Null    -> 保留相应 NULL 语义
```

相关实现：

- `dbms/src/Storages/DeltaMerge/FilterParser/FilterParser.cpp`
- `dbms/src/Storages/DeltaMerge/Filter/RSOperator.h`
- `dbms/src/Storages/DeltaMerge/File/DMFilePackFilter.cpp`
- `dbms/src/Storages/DeltaMerge/Index/RSResult.h`
- `dbms/src/DataStreams/FilterTransformAction.cpp`

`FilterTransformAction` 在 block 的 RS 结果为 `All` 时直接构造全 true filter，不再执行真实表达式。因此，任何新索引都必须严格保证 `All` 表示 pack 中所有可见行都满足谓词；它不能仅表示“被索引保留的值全部满足谓词”。

### 问题场景

考虑一个 `settle_time DATETIME` 列：

- 正常值位于过去 90 天；
- `1/10000` 的记录使用 `2100-01-01 00:00:00` 表示尚未结算；
- stable pack 默认约 8192 行；
- 哨兵值均匀散布在表中。

一个 pack 至少包含一个哨兵值的概率为：

```text
1 - (1 - 1/10000)^8192 ≈ 55.92%
```

因此约 55.92% 的 pack 会形成：

```text
min = 正常历史时间
max = 2100-01-01 00:00:00
```

对于查询：

```sql
settle_time >= L AND settle_time <= U
```

普通 min-max 只有在以下任一条件成立时才能排除 pack：

```text
pack.max < L
pack.min > U
```

2100 哨兵值会破坏第一种证明。对于位于最新时间端的短时间查询，大量原本完全早于 `L` 的历史 pack 将被错误保留为 `Some`。如果正常时间在 pack 内具有较好的局部有序性，查询最近 90 天中的最新 3 小时，单独考虑时间条件时，理论读取 pack 比例可能从约 `1/720 = 0.139%` 增加到约 55.98%。

若正常时间本身在每个 pack 内完全随机分布，则普通 min-max 原本已经缺少过滤性，trim 索引的边际收益也会较小。因此上线验证必须同时覆盖时间局部性较好和较差的两类数据。

### 关键约束

1. 不能把某个特定业务哨兵值硬编码进通用 min-max，否则查询该值时可能产生错误剪枝。
2. Reader 必须支持历史 DMFile；上线不能依赖一次性重写全量 stable 数据。
3. mixed-version 或回滚过程中，旧 Reader 必须能够忽略新索引并继续使用普通 min-max。
4. `TIMESTAMP` literal 在当前 FilterParser 中会根据请求时区转换为 UTC；`DATETIME` 和 `DATE` 不具有相同的时区语义。
5. trim 索引必须与普通 min-max 使用相同的 pack 边界、NULL 规则和 delete mark 规则。
6. `RSResult::All` 具有跳过行级过滤的执行语义，不能把它当作普通的统计标签。

## 术语

| 术语 | 定义 |
| --- | --- |
| 普通 min-max | 当前 DMFile 为 pack 保存的完整非 NULL、非删除值 min/max |
| 有效日期区间 `E` | 构建某一份 trim 索引时使用并持久化的半开区间 `[lower, upper)` |
| trim value | 位于 `E` 内、参与 trim min/max 计算的值 |
| trimmed value | 位于 `E` 外、未参与 trim min/max 计算的非 NULL、非删除值 |
| `pack_marks` | min-max index 中原 `has_null_marks` 对应的每 pack `UInt8` 数组；bit 0 为 NULL，trim index 额外使用 bit 1/2 |
| `has_trimmed_low` | `pack_marks & 0x02`，表示存在 `< lower_bound` 的非 NULL、非删除值 |
| `has_trimmed_high` | `pack_marks & 0x04`，表示存在 `>= upper_bound` 的非 NULL、非删除值 |
| 查询域 `Q` | 某个列谓词可能匹配的全部非 NULL 时间值集合 |
| trim eligible | 已证明 low/high 两类 trimmed value 对该谓词各自具有一致匹配语义，能够结合 flags 安全修正 trim RSResult |

## 目标

1. 在稀疏极端日期均匀散布的场景中，恢复日期时间范围查询的 pack 级过滤能力。
2. 对 trim eligible 的谓词生成正确的 `None`、`Some` 和 `All`，不改变查询结果。
3. 允许不同代际 DMFile 使用不同有效日期区间，并由 Reader 按文件安全选择索引。
4. 老 DMFile、未知索引版本、缺失或损坏的 trim 元数据都能回退普通 min-max。
5. 默认关闭时不影响现有查询热路径；开启后仅对相关日期时间列增加有限的写入、元数据和 cache 开销。
6. 提供足够的指标验证索引命中、回退原因、pack 剪枝收益和写入成本。

## 非目标

- 不修改 TiDB SQL 语义或时间类型语义。
- 不要求用户修改 DDL 或显式创建索引。
- 不在第一版支持 `TIME`、duration、字符串或数值列。
- 不在第一版为 DMFile V1/V2 写入 trim 索引。
- 不在第一版对历史 DMFile 执行主动 backfill；历史文件通过后续 merge delta、split、compact 或 GC 自然重写。
- 不在第一版支持 `OR`、`NOT`、`!=`、`NOT IN`、表达式列或包含 cast 的 trim 谓词分析。
- 不把有效日期区间作为用户级表属性或 DDL 属性暴露。

## 正确性基础

设某 pack 中该列所有参与普通 min-max 的值集合为 `D`，有效日期区间为 `E`，trim 索引统计的集合为：

```text
D_trim = D ∩ E
```

设列谓词的非 NULL 匹配集合为 `Q`。当：

```text
Q ⊆ E
```

时，有：

```text
D ∩ Q = (D ∩ E) ∩ Q = D_trim ∩ Q
```

因此：

- trim min-max 证明 `D_trim ∩ Q` 为空时，可以安全返回 `None`；
- trim min-max 不能仅凭 `D_trim ⊆ Q` 返回最终 `All`，因为 `D - E` 中可能仍有不匹配值；
- 对 equality、IN 和 bounded range，low/high trimmed value 均不匹配，因此任一方向存在 trimmed value 时，trim 返回的 `All` 必须降级成 `Some`；
- NULL 仍由 trim 索引的 `has_null` 标记参与 `RSResult` 组合，不能算作 trimmed value。

方向 flags 还允许安全处理边界值位于 `E` 内的单边范围：

| 谓词类型 | low trimmed value | high trimmed value |
| --- | --- | --- |
| `col >= T` / `col > T` | 一定不匹配 | 一定匹配 |
| `col <= T` / `col < T` | 一定匹配 | 一定不匹配 |

因此，trim 原始结果为 `None` 但存在“必定匹配”的 trimmed value 时，需要降级为 `Some`；trim 原始结果为 `All` 但存在“必定不匹配”的 trimmed value 时，也需要降级为 `Some`。这里的“降级”表示放弃 pack 级确定性并保留行级过滤，不尝试从 `None` 升级为 `All`。

以下反例说明为什么不能仅检查 SQL literal 是否位于有效区间。

```text
E = [1900, 2100)
pack = {2100-01-01}
predicate = col >= 2020-01-01
```

literal 2020 位于 `E` 内，但谓词的查询域为 `[2020, +∞)`，包含 2100。若使用空的 trim min-max 直接返回 `None`，将错误漏行。本设计通过 `has_trimmed_high` 识别必定匹配的高端值，把 `None` 修正为 `Some`，从而安全支持该单边比较。

另一个反例说明 low/high pack marks 的必要性：

```text
E = [1900, 2100)
pack = {2021-01-01, 2100-01-01}
predicate = col BETWEEN 2020-01-01 AND 2022-01-01
```

trim 后 `min=max=2021-01-01`，但 pack 中的 2100 不匹配。trim rough check 应返回 `Some`，而不是 `All`。

## 设计

### 总体架构

```text
DMFile write path
  -> ordinary MinMaxIndex: 所有非 NULL、非删除值
  -> TrimMinMaxIndex: E 内的非 NULL、非删除值
  -> TrimMinMaxMetaBlock:
       - format version
       - per-column encoded lower/upper bound
       - pack count
       - trim index subfile descriptor
  -> trim index pack_marks[pack_count]:
       - bit 0: has_null
       - bit 1: has_trimmed_low
       - bit 2: has_trimmed_high

Query DAG
  -> FilterParser
       - 构建现有 RSOperator
       - 归一化谓词类型和时间边界
  -> DMFilePackFilter（逐 DMFile）
       - 谓词的边界位于 stored E 且 low/high 语义可证明：选择 trim
       - 否则：选择 ordinary min-max
  -> roughCheck
       - trimmed matching value + None: Some
       - trimmed non-matching value + All: Some
       - 其他 None/All: 保持
       - Some: Some
```

### 有效日期区间

第一版默认区间固定为：

```text
[1900-01-01 00:00:00, 2100-01-01 00:00:00)
```

采用半开区间而不是 `2099-12-31 23:59:59` 闭区间，原因是半开区间可以无歧义地覆盖 `DATETIME(1..6)` 的小数秒，例如 `2099-12-31 23:59:59.999999`。

边界以列内部编码形式持久化，而不是时间字符串：

- `DATE` 对应 `DataTypeMyDate` 的 packed value；
- `DATETIME` 和 `TIMESTAMP` 对应 `DataTypeMyDateTime` 的 packed value；
- 上下界的字段类型必须与索引所属列的 nested type 一致；
- `format_version = 1` 的格式契约固定将边界解释为 `[lower, upper)`，metadata 不再单独保存边界模式。

`TIMESTAMP` 的查询 literal 沿用当前 `FilterParser::convertFieldWithTimezone`，在 Rough Set 分析前转换成 UTC packed value。`DATETIME` 和 `DATE` 按日历值比较，不把“UTC+0”作为其类型语义。

### TrimMinMaxIndex 数据模型

trim min-max 复用普通 `MinMaxIndex` 的核心表示：

```text
pack_marks[pack_count]       // 物理位置与现有 has_null_marks 相同
has_value_marks[pack_count]
minmaxes[pack_count * 2]
```

`pack_marks` 的 V1 bit layout 为：

| bit | mask | ordinary min-max | trim min-max |
| --- | --- | --- | --- |
| 0 | `0x01` | `has_null` | `has_null` |
| 1 | `0x02` | 必须为 0 | `has_trimmed_low` |
| 2 | `0x04` | 必须为 0 | `has_trimmed_high` |
| 3..7 | `0xf8` | 必须为 0 | 必须为 0，保留 |

其他字段语义为：

- `has_value_marks`：该 pack 是否存在至少一个位于 `E` 内的非 NULL、非删除值；
- `minmaxes`：只对 `E` 内值计算；
- NULL、low/high flags 和 trim min/max 使用相同 pack 顺序，全部位于同一个 trim index payload。

普通 `.idx` 的字节布局和已有内容不变：历史文件的 mark 本来就是 `0x00` 或 `0x01`。代码中把成员概念从 `has_null_marks` 泛化为 `pack_marks` 后，所有 NULL 判断必须使用 `(pack_marks[i] & 0x01) != 0`，不能继续把整个 byte 当作 bool，否则 low/high bit 会被误判为 NULL。建议只通过 `hasNull()`、`hasTrimmedLow()` 和 `hasTrimmedHigh()` accessor 读取。

`TrimMinMaxIndex` 不定义为 `MinMaxIndex` 的子类。当前 `MinMaxIndex` 的状态是 private，`read()` 固定构造基类，`checkCmp` 是非虚模板方法，查询路径又通过 `MinMaxIndexPtr` 调用；为继承引入虚接口和专用工厂不会简化磁盘格式。第一版使用组合：

```cpp
struct TrimMinMaxIndex
{
    MinMaxIndexPtr minmax;

    RSResults roughCheck(
        TrimPredicateClass predicate_class,
        size_t start_pack,
        size_t pack_count) const;
};
```

`MinMaxIndex` 负责通用序列化和原始 min-max check，trim wrapper 或 `DateRange` Operator 读取 pack mark accessor 并修正 `None` / `All`。wrapper 不保存额外 per-pack 数组。

trim index 使用独立 subfile 名称，逻辑形式为：

```text
<column-stream>.trim.idx
```

在 DMFile V3 中，该小文件与普通 min-max 和 mark 一样写入 merged file，并由 `MergedSubFileInfo` 保存物理文件号、offset 和 size。

不把 trim 数据追加到现有 `.idx`。当前 `MinMaxIndex::read` 要求实际消费字节数与普通 `index_bytes` 完全一致，直接扩展现有文件会破坏旧 Reader。

### DMFileMetaV2 元数据

第一版新增独立 MetaV2 block：

```text
BlockType::TrimMinMaxMeta
```

逻辑 protobuf 结构如下：

```protobuf
message TrimMinMaxColumnMeta {
    optional int64 col_id = 1;
    optional uint32 format_version = 2;

    // 与列 nested type 相同的内部编码。
    optional bytes lower_bound = 3;
    optional bytes upper_bound = 4;

    optional uint64 pack_count = 5;
    optional string index_file_name = 6;
    optional uint64 index_file_size = 7;
}

message TrimMinMaxMeta {
    repeated TrimMinMaxColumnMeta columns = 1;
}
```

实际字段编号在实现时按 protobuf 兼容规则分配；以上结构定义的是数据契约。

元数据约束：

1. `format_version = 1` 固定使用半开区间 `[lower_bound, upper_bound)`；未来改变边界语义必须升级版本，Reader 遇到未知版本时回退 ordinary min-max。
2. bounds 必须能按列的 nested type 解码，并满足 `lower_bound < upper_bound`。
3. `pack_count` 必须等于 DMFile 的 pack 数量。
4. `index_file_size` 必须与 `MergedSubFileInfo` 或独立 subfile 实际大小一致。
5. trim index 解码出的 `pack_marks`、`has_value_marks` 和 min/max cell 数量必须一致，并等于 `pack_count`。
6. trim index 的 `pack_marks` bit 3..7 必须为 0。
7. trim metadata 和 index subfile 必须由同一个 immutable DMFile generation 原子生成和发布；Reader 不得跨 DMFile 复用或组合它们。
8. 同一 DMFile 的不同时间列可以有不同 metadata；Reader 不能假设 file 级全局区间。
9. metadata 缺失、版本未知或任一结构校验失败时，不使用 trim 索引。

TrimMinMaxMeta block 由 DMFileMetaV2 checksum 保护，trim index subfile 使用现有 DMFile checksum 机制；两者再结合 pack 数量、pack mark 合法性、subfile 位置和大小等结构校验处理物理损坏。Meta block 不保存任何 per-pack trim flags。

不直接给 `PackStat` 或 `PackProperty` 增加字段。MetaV2 当前以 POD 原始布局序列化这两个结构，修改 `sizeof` 会破坏旧格式。新增独立 block 允许旧 Reader 按当前逻辑忽略未知 block，并继续读取 ordinary min-max。

第一版不复用 `DMFileIndexInfo.indexes` 的 `IndexFileKind` 扩展点。当前旧代码会在 `ColumnStat::integrityCheckIndexInfoV2` 中拒绝未知 index kind，直接增加枚举不满足滚动降级要求。后续如果通用本地索引框架支持“忽略未知 index kind”，可以再迁移 metadata 表达方式。

### 写入路径

`DMFileWriter::Stream` 为支持日期时间类型的列增加可选 trim `MinMaxIndex` builder。普通和 trim min-max 在同一次 pack 遍历中计算，避免对列执行两次完整扫描；这里复用 builder 和 serializer，不通过继承扩展 `MinMaxIndex`。

伪代码：

```cpp
UInt8 trim_pack_mark = 0;

for (row : pack)
{
    if (isDeleted(row))
        continue;

    if (isNull(row))
    {
        ordinary.has_null = true;
        trim_pack_mark |= 0x01; // has_null
        continue;
    }

    ordinary.updateMinMax(row.value);

    if (lower <= row.value && row.value < upper)
        trim.updateMinMax(row.value);
    else if (row.value < lower)
        trim_pack_mark |= 0x02; // has_trimmed_low
    else
        trim_pack_mark |= 0x04; // has_trimmed_high
}
```

每写完一个 pack：

1. ordinary min-max 按现有流程追加一个 cell；
2. trim min-max 追加一个 cell；若没有有效值，则 `has_value=false`；
3. 把 `trim_pack_mark` 追加到 trim index 的 `pack_marks`；
4. finalize 时写 trim subfile 和 `TrimMinMaxMeta` block。

实现上为 `MinMaxIndex` 增加内部 `appendPack(pack_mark, has_value, min, max)` 或等价 builder API，由 ordinary 和 trim Writer 共用。ordinary 路径只允许 `pack_mark & ~0x01 == 0`；trim 路径允许 `0x01 | 0x02 | 0x04`。反序列化仍复用通用 payload reader，trim loader 随后校验 mark mask 和 pack count。

以下列不生成 trim 索引：

- handle、version、delete mark 等内部列；
- `TIME` 和 duration；
- 非 `MyDate` / `MyDateTime` nested type；
- 空 DMFile；
- 写入开关关闭时的所有列。

可以在 finalize 时检测整个 DMFile 某列是否存在任何 trimmed value。若所有 `pack_marks & 0x06` 都为 0，普通 min-max 与 trim min-max 对非 NULL 值等价，可以不持久化 trim subfile 和 metadata，从而避免对无异常值文件增加存储开销。bit 0 的 NULL 标记不影响该判断。

### 查询域分析

当前 `FilterParser` 把每个比较表达式独立转换成 `GreaterEqual`、`LessEqual` 等 Operator。仅凭单个 literal 不能判断完整查询域是否属于有效区间，因此需要新增时间列查询域归一化步骤。

第一版支持以下 trim eligible 形式：

```sql
time_col = T
time_col IN (T1, T2, ...)
time_col >= L AND time_col <= U
time_col > L AND time_col < U
time_col >= L
time_col > L
time_col <= U
time_col < U
```

其中：

- equality 的查询域为单点 `{T}`；
- IN 的所有非 NULL 值都必须位于 stored `E`；
- bounded range 的上下界在类型转换和时区转换后必须形成 `Q ⊆ E`；
- lower-bounded range 的下界必须位于 stored `E`，此时 low trimmed value 一定不匹配、high trimmed value 一定匹配；
- upper-bounded range 的上界必须位于 stored `E`，此时 low trimmed value 一定匹配、high trimmed value 一定不匹配；
- 上下界可以来自同一个 `LogicalAnd`，也可以来自 `DAGQueryInfo::filters` 和 `pushed_down_filters` 最终形成的顶层 AND；
- 若同一列出现多个上下界，选择语义上最强的下界和上界；
- 任何涉及 OR、NOT、NotEqual、NotIn、IsNull、函数或 cast 的分支，第一版不生成 trim 查询域。

建议新增 `DateRange` RSOperator 表示已经归一化的日期范围，包括 bounded 和单边范围。该 Operator 只影响 rough check；真实行级 filter 仍由原始 DAG expression 构建，不修改 SQL 执行表达式。

为了让 `DMFilePackFilter` 按 DMFile 选择 normal 或 trim index，扩展索引请求接口，使 Operator 能声明：

```cpp
struct RSIndexRequest
{
    ColId col_id;
    RSIndexKind preferred_kind; // Normal 或 PreferTrim
    std::optional<DateQueryDomain> query_domain;
};
```

`RSCheckParam` 分别保存 normal 和 trim index，避免同一列同时参与 trim eligible range 与其他普通谓词时发生覆盖：

```cpp
struct RSCheckParam
{
    ColumnIndexes normal_indexes;
    TrimColumnIndexes trim_indexes; // value 内含组合式 TrimMinMaxIndex wrapper
    TrimMinMaxMetas trim_metas;
};
```

若一个 temporal Operator 的查询域在某个 DMFile 上满足 trim 条件，优先只加载 trim index；同一列还有其他需要普通 min-max 的 Operator 时，允许两类索引同时存在。索引 cache 使用不同 key，至少包含稳定的 DMFile identity、列 ID 和索引种类。由于 DMFile 是 immutable generation，同一 identity 下的 bounds 和包含 pack marks 的 index payload 不会独立变化。

### 按 DMFile 选择索引

对每个时间列请求和每个 DMFile 独立执行：

```cpp
if (!trim_read_enabled)
    choose NORMAL;
else if (!trim_meta_exists(col_id))
    choose NORMAL;
else if (!readerSupports(trim_meta.format_version))
    choose NORMAL;
else if (!metaAndIndexAreConsistent(trim_meta))
    choose NORMAL;
else if (!query_domain.isTrimEligible(trim_meta.range))
    choose NORMAL;
else
    choose TRIM;
```

`isTrimEligible` 不等价于简单的 `Q ⊆ E`。equality、IN 和 bounded range 仍要求完整匹配集合位于 `E`；单边 range 则要求有限边界位于 `E`，并把谓词分类结果传给 rough check，以确定 low/high trimmed value 分别是“必定匹配”还是“必定不匹配”。

选择必须使用 `trim_meta.range`，不能使用当前进程默认范围。由此可以安全支持：

```text
DMFile A: E=[1900, 2100)，使用 trim
DMFile B: 无 trim，使用 normal
DMFile C: 未来版本 E=[1800, 2200)，按 C 的 metadata 决定
```

Reader 不支持 metadata 版本或校验失败时，行为是回退 ordinary min-max，而不是返回 `Some` 之外的推测结果。若底层 index payload checksum 已明确损坏，仍沿用 DMFile 的数据损坏处理策略，不静默掩盖物理损坏。

### Trim rough check 和 `pack_marks`

Reader 对每个 pack 解码：

```cpp
const UInt8 pack_mark = trim_index.minmax->packMark(pack_id);
const bool has_null = (pack_mark & 0x01) != 0;
const bool has_trimmed_low = (pack_mark & 0x02) != 0;
const bool has_trimmed_high = (pack_mark & 0x04) != 0;
```

根据谓词类型计算 trimmed value 是否包含必定匹配或必定不匹配的值：

| 谓词类型 | `trimmed_match_exists` | `trimmed_nonmatch_exists` |
| --- | --- | --- |
| equality / IN / bounded range，且 `Q ⊆ E` | false | `has_trimmed_low || has_trimmed_high` |
| lower-bounded range，边界位于 `E` | `has_trimmed_high` | `has_trimmed_low` |
| upper-bounded range，边界位于 `E` | `has_trimmed_low` | `has_trimmed_high` |

trim min-max 先按现有 RoughCheck 规则计算结果，再应用以下保守修正：

| trim 原始结果 | 条件 | 最终结果 |
| --- | --- | --- |
| `None` | `trimmed_match_exists=false` | `None` |
| `None` | `trimmed_match_exists=true` | `Some` |
| `NoneNull` | `trimmed_match_exists=false` | `NoneNull` |
| `NoneNull` | `trimmed_match_exists=true` | `SomeNull` |
| `Some` / `SomeNull` | 任意 | 保持不变 |
| `All` | `trimmed_nonmatch_exists=false` | `All` |
| `All` | `trimmed_nonmatch_exists=true` | `Some` |
| `AllNull` | `trimmed_nonmatch_exists=false` | `AllNull` |
| `AllNull` | `trimmed_nonmatch_exists=true` | `SomeNull` |

该规则只把确定结果降级为 `Some`，不会根据 flags 把 `None` 升级为 `All` 或把 `Some` 升级为确定结果。这样即使 trim pack 没有任何 in-range value，也能安全处理单边查询：例如 pack 全部为 2100，`col >= 2020` 的 trim 原始结果可能是 `None`，但 `has_trimmed_high` 会将其修正为 `Some`；bounded range 查询则仍可把该 pack 保持为 `None` 并跳过。

### NULL、删除和 MVCC

trim index 延续当前普通 min-max 的规则：

- NULL 不参与 min/max；
- 非删除 NULL 会设置 trim 的 `has_null`；
- NULL 不设置 `has_trimmed_low` 或 `has_trimmed_high`；
- delete mark 对应值不参与 normal、trim，也不设置 `pack_marks` 的 low/high bit；
- pack 中没有非删除有效值时设置 `has_value=false`。

这保证 trim 与普通 min-max 的 RS 三值语义一致。若未来普通 min-max 对 MVCC 可见值集合的定义发生变化，trim 生成必须同步变化，不能形成两套不同的行集合。

### 配置与开关

第一版提供两个内部设置：

```text
dt_enable_trim_minmax_write
dt_enable_trim_minmax_read
```

- write 开关控制新 DMFile 是否生成 trim index；
- read 开关控制 Reader 是否选择 trim；
- 两者默认先关闭，通过 canary 逐步开启；
- 关闭 read 后，已写入的 trim metadata 和 subfile 被忽略，立即回退 ordinary min-max；
- 有效区间第一版不是动态配置项，避免同一进程内配置漂移，但仍持久化进 metadata，为未来格式演进保留正确性。

### 可观测性

新增按查询或实例聚合的计数和耗时：

```text
trim_minmax_index_load_count
trim_minmax_index_load_bytes
trim_minmax_index_load_time
trim_minmax_selected_packs
trim_minmax_none_packs
trim_minmax_some_packs
trim_minmax_all_packs
trim_minmax_none_downgraded_packs
trim_minmax_all_downgraded_packs
trim_minmax_fallback_count{reason}
trim_minmax_write_bytes
trim_minmax_write_time
```

`fallback reason` 至少区分：

```text
disabled
no_meta
unsupported_version
predicate_boundary_outside_range
unsupported_expression
metadata_mismatch
index_missing
```

Debug 日志记录 DMFile、列 ID、stored range、query domain、选择的索引类型和 pack 过滤率。长期应把 trim pack 统计并入 `ScanContext`，以便 `EXPLAIN ANALYZE` 展示，但第一阶段可以先通过 ProfileEvents、实例 metric 和 debug log 完成验证。

## 兼容性与不变量

### 查询正确性不变量

1. trim 索引不能让任何原本匹配的行消失。
2. trim 索引不能把不匹配的 trimmed value 通过 `All` 带入结果。
3. equality、IN 和 bounded range 只有在 `Q ⊆ stored E` 时才能选择 trim；单边 range 只有在有限边界位于 stored `E` 且 low/high 匹配语义已知时才能选择 trim。
4. Reader 无法验证 metadata 与 index payload 一致时不得使用 trim。
5. 行级 filter 始终保留；只有最终 RSResult 为严格正确的 `All` 时才允许跳过。
6. `format_version = 1` 的边界语义始终为 `[lower_bound, upper_bound)`；不能由运行时配置重新解释。
7. NULL 语义只读取 `pack_marks & 0x01`；low/high bit 不能影响普通 min-max 的 NULL 判断。

### 磁盘格式兼容

- 普通 `.idx` 格式不变；旧 Reader 继续读取普通 min-max。
- 普通 `.idx` 的 pack mark 仍只能为 `0x00` / `0x01`；trim 扩展 bit 只出现在独立 `.trim.idx`。
- trim 使用独立 subfile，旧 Reader 不会把额外字节解释成普通索引。
- trim metadata 使用独立 MetaV2 block；当前 MetaV2 读取逻辑会忽略未知 block。
- 新 Reader 读取不含 trim block 的旧 DMFile 时回退 normal。
- `format_version` 同时版本化序列化结构和边界语义；不支持的版本回退 normal。
- 第一版不修改 raw `PackStat` / `PackProperty` 布局。
- 第一版只在 V3 / MetaV2 写入，避免同时扩展 V1/V2 metadata 格式。

### 滚动升级与降级

推荐顺序：

1. 先部署能够识别或忽略 trim metadata 的新 Reader，read/write 均关闭。
2. 开启少量节点 write，生成新 DMFile；read 仍关闭。
3. 验证新 DMFile 能被旧版本安全忽略后，再 canary 开启 read。
4. 扩大 read/write 范围。

降级时先关闭 read，再关闭 write。已经存在的 trim subfile 和 metadata 不影响普通 min-max。必须在兼容测试中验证“新写旧读”；若目标旧版本无法安全忽略新的 merged subfile 或 Meta block，则 write 开关不能在混部阶段开启。

## 性能和资源开销

### 索引空间

对于 `MyDateTime`，每 pack 的 trim min/max 约包含：

```text
min + max        16 bytes
pack_marks        1 byte
has_value_marks   1 byte
```

按当前 `MinMaxIndex` 的字节数组表示，未压缩量级约为每 pack 每列 18 bytes，8192 行一个 pack 时约为：

```text
18 / 8192 ≈ 0.0022 bytes/row/column
```

low/high flags 复用 trim min-max 已有的每 pack `has_null_marks` byte，因此相对于一份 trim min-max payload 不再增加额外字节，也不会让 MetaV2 随 pack 数增长。一个默认约 100 万行、123 个 pack、5 个时间列的 DMFile，相比把 flags 单独存放在 metadata 的方案，可减少约 `123 × 5 = 615` bytes MetaV2 内容。

### 写入 CPU

若独立调用两次 min/max 扫描，日期时间列的索引构建 CPU 可能接近翻倍。设计要求在一次遍历中同时更新 normal 和 trim，公共路径只增加两次边界比较和一个条件分支。

### 读取与 cache

- trim eligible 且该列没有其他 normal index 请求时，只加载 trim，不加载 ordinary min-max；
- fallback 查询只加载 ordinary；
- 同一列同时存在 trim eligible 和其他普通谓词时可能加载两份索引；
- trim cache key 与 ordinary 分离；pack marks 已包含在 `MinMaxIndex::byteSize()` 对应的第一个 byte array 中，不单独计算 cache weight；
- 无 trimmed value 的 DMFile 不持久化 trim，可避免无收益开销。

## 分阶段实施与发布

### 阶段 A：格式和兼容性

- 增加 `TrimMinMaxMeta` 的 protobuf、MetaV2 block 和解析校验。
- 增加 trim subfile 命名、merged file 定位和 cache key。
- 将 `has_null_marks` 的内部读取收敛到 pack mark accessor，验证 ordinary min-max 只解释 bit 0。
- 新 Reader 能读取旧文件，并在所有 trim 异常情况下回退 normal。
- read/write 开关默认关闭。
- 完成新写旧读、旧写新读、CN/存算分离文件路径和 checksum 测试。

### 阶段 B：写入索引

- 在 `DMFileWriter` 中一次遍历生成 normal、trim 和 trim `pack_marks`。
- 仅为 V3 的 `MyDate` / `MyDateTime` 用户列生成。
- write canary，观察写吞吐、CPU、DMFile 大小和 meta 大小。

### 阶段 C：查询域和读路径

- 增加 top-level AND 时间范围归一化和 `DateRange` Operator。
- 使用组合式 trim wrapper 调用通用 `MinMaxIndex`，不引入基类虚接口或派生类反序列化。
- 支持 equality、IN、bounded range 和单边 range 的 trim eligibility。
- 按 DMFile stored range 选择索引。
- 根据 low/high flags 实现 `None -> Some`、`NoneNull -> SomeNull`、`All -> Some` 和 `AllNull -> SomeNull` 的保守修正。
- read canary，对比查询结果、扫描行数和 pack 过滤率。

### 阶段 D：默认开启和自然迁移

- 在兼容性与性能门槛满足后逐步默认开启 read/write。
- 旧 DMFile 继续使用 normal，不做全量 backfill。
- 通过现有 merge delta、split、compact 和 GC 自然增加 trim 覆盖率。
- 保留 read/write kill switch 至少一个完整发布周期。

## 验证策略

### 单元测试

#### TrimMinMaxIndex

至少覆盖以下 pack：

```text
全部位于 E 内
全部低于 E
全部高于 E
同时存在低端和高端 outlier
正常值 + 2100 哨兵
只有 NULL
NULL + 正常值 + outlier
delete mark + 正常值 + outlier
没有任何有效值
```

验证 min/max、`has_value`、pack mark accessor 和序列化往返，至少覆盖 `0x00`、`0x01`（NULL）、`0x02`（low）、`0x04`（high）、`0x06`（low + high）和 `0x07`（NULL + low + high）。trim V1 Reader 必须拒绝 bit 3..7 非 0 的 index payload；普通历史 `.idx` 的 `0x00` / `0x01` 必须保持兼容。

#### RSResult

重点验证：

```text
pack={2021, 2100}, query=[2020, 2022] -> Some，不能是 All
pack={2100}, query=[2020, 2022]       -> None
pack={2021}, query=[2020, 2022]       -> All
pack={NULL, 2021}, query=[2020, 2022] -> AllNull 或需要行级过滤的等价结果
pack={2100}, query>=2020               -> Some，不能是 None
pack={1800}, query>=2020               -> None
pack={1800}, query<=2020               -> Some，不能是 None
pack={2100}, query<=2020               -> None
pack={1800, 2100}, query>=2020         -> Some
```

#### 查询域分析

可使用 trim：

```sql
col = '2020-01-01'
col IN ('2020-01-01', '2021-01-01')
col >= '2020-01-01' AND col <= '2020-01-02'
col >= '2020-01-01'
col < '2020-01-01'
```

必须 fallback：

```sql
col >= '2200-01-01'
col <= '1800-01-01'
col != '2020-01-01'
NOT (col BETWEEN ...)
col BETWEEN ... OR status = 1
col BETWEEN ... OR col IS NULL
CAST(col AS ...) = ...
```

### 时间类型测试

- `DATE`、`DATETIME(0)`、`DATETIME(3)`、`DATETIME(6)`；
- `TIMESTAMP` 在 UTC、固定 offset、命名时区以及 DST 边界；
- lower/upper 边界本身；
- `2099-12-31 23:59:59.999999`；
- `2100-01-01 00:00:00`；
- zero date、非法日期兼容值等区间外 packed value；
- Nullable 与 NotNull。

### DMFile 格式和兼容测试

- 旧 DMFile -> 新 Reader；
- 新 DMFile -> 兼容范围内旧 Reader；
- trim metadata 缺失、未知版本、错误 pack_count；
- trim index 的 pack marks 数量不等于 pack_count、bit 3..7 非 0；
- bounds 无法解码、`lower_bound >= upper_bound`；
- metadata 或 index subfile checksum 不匹配；
- local disk 和 disaggregated storage；
- merged subfile reopen、clone、restore、GC 和 segment replacement；
- read/write 开关的在线切换与回退。

### 查询结果测试

对同一数据集分别运行：

```text
trim read disabled
trim read enabled
强制 fallback normal
```

比较完整结果集，而不只比较行数。覆盖 SELECT、聚合、TopN、LIMIT、并发读取、分区表和多 DMFile 混合版本。

### 性能测试

构造以下数据：

- pack size 8192；
- 正常时间覆盖 90 天并具有局部有序性；
- `1/10000` 的行使用 2100 哨兵并均匀散布；
- 查询最新连续 3 小时。

对比：

```text
ordinary min-max
ordinary + trim disabled
trim enabled
无 outlier 基线
```

至少采集：

- RS none/some/all pack 数；
- DMFile scanned/skipped rows 和 bytes；
- index load bytes/time/cache hit；
- 查询 p50/p95/p99 latency；
- merge delta / compact 写吞吐和 CPU；
- DMFile data、index 和 meta 大小。

验收时不把理论 403 倍 pack 降幅作为硬门槛，因为它依赖时间局部性；硬门槛应是查询结果完全一致、无 outlier workload 无显著退化，并且目标数据集的 scanned rows 明显接近无 outlier 基线。

## 风险与缓解

### 错误使用当前默认区间解释旧索引

风险：有效区间在产品迭代中改变，新 Reader 对旧 trim 索引产生 false negative。

缓解：每列每 DMFile 持久化实际上下界；eligibility 只使用 stored range；V1 格式固定使用半开区间。metadata 和包含 pack marks 的 index payload 随同一个 immutable DMFile generation 原子发布，并通过现有 checksum 和结构校验发现损坏或错误组合。

### `None` 或 `All` 未按方向 flags 修正

风险：trim 忽略 outlier 后错误保留 `None` 会漏掉匹配的 trimmed rows；错误保留 `All` 会使不匹配的 trimmed rows 绕过行级过滤。

缓解：持久化 per-column/per-pack low/high flags；存在必定不匹配的 trimmed value 时把 `All` 降级为 `Some`，存在必定匹配的 trimmed value 时把 `None` 降级为 `Some`，并增加专门的结果一致性测试。

### 查询域分析不完整

风险：错误分类谓词后，Reader 可能把 low/high trimmed value 的匹配方向判断反，或在 OR 查询中错误使用 trim。

缓解：第一版白名单支持 equality、IN、top-level AND bounded range 和边界位于 `E` 内的单边 range；显式测试四种比较方向以及 low/high 同时存在的 pack，其他表达式全部 fallback。

### mixed-version 读取失败

风险：旧 Reader 无法忽略新 metadata 或 merged subfile。

缓解：独立 Meta block 和 subfile，不修改普通 `.idx` 或 raw PackStat；write 在新写旧读验证通过前保持关闭。

### 写入 CPU 或 cache 开销

风险：额外索引抵消查询收益，尤其是无 outlier workload。

缓解：一次遍历生成两个索引；全文件无 trimmed value 时不持久化 trim；normal 与 trim lazy load；设置独立 read/write kill switch。

### 时间类型边界或时区不一致

风险：Writer 和 Reader 对 bounds 的 packed 表达或 TIMESTAMP literal 转换不一致。

缓解：metadata 保存 typed packed bounds；Reader 在现有 timezone conversion 后判断查询域；覆盖 FSP、UTC、offset 和 DST 测试。

### Trim index 和 MetaV2 空间增长

风险：额外 trim index 增加 DMFile 大小；大量时间列仍会为 MetaV2 增加固定大小的 column metadata。

缓解：low/high flags 复用 trim index 已有的 pack mark byte，MetaV2 不保存 per-pack 数据；无 outlier 列不写 trim index 和 metadata；增加 index/meta 大小及 parse 时间指标。

## 备选方案

### 使用 NULL 表示未结算

当前普通 min-max 已排除 NULL，因此这是业务可修改数据模型时最简单的方案。但存量 schema、应用兼容和其他业务哨兵场景可能无法统一迁移，不能替代存储层通用优化。

### 缩小 pack size

减少 pack 行数可以降低 outlier 污染概率，但会增加 pack 数、索引大小、mark 和读写调度开销，并且无法根治极端值污染。

### 在普通 min-max 中硬编码忽略 2100

查询 2100 或覆盖 2100 的范围时会产生错误结果，且无法推广到其他哨兵值，因此拒绝。

### 只把 trim 作为额外的 None gate

该方案最容易保证正确性：trim 只排除 pack，其他结果完全沿用普通 min-max。但它需要同时加载普通和 trim，并且不能恢复正确的 `All`。本设计通过在 trim index pack marks 中持久化 low/high flags，允许 trim 在 eligible 查询中安全替代 ordinary min-max，因此选择完整 RSResult 方案。

### 保存两个独立 bitmap

分别保存 `has_trimmed_low_bitmap` 和 `has_trimmed_high_bitmap` 需要维护两份长度、尾部 bit、寻址和独立存储位置。第一版复用 trim min-max 已有的 `has_null_marks` byte：bit 0 保持 NULL 语义，bit 1/2 表达 low/high；不新增 per-pack byte，也不把 bitmap 放入 MetaV2。

### 让 `TrimMinMaxIndex` 继承 `MinMaxIndex`

当前 `MinMaxIndex` 的状态是 private，`read()` 固定构造基类，`checkCmp` 是非虚模板方法，查询路径通过 `MinMaxIndexPtr` 静态调用。为了继承而增加虚析构、虚接口、protected 状态和派生类工厂，会扩大普通 min-max 热路径和 cache 的改动面，却不改变磁盘布局。因此第一版拒绝继承，使用 `MinMaxIndex` payload 加轻量 trim wrapper 的组合方式。

## 已确定的设计边界

- 有效区间使用半开区间 `[1900-01-01, 2100-01-01)`。
- 实际 bounds 按列、按 DMFile 持久化，Reader 不依赖当前默认配置解释旧索引。
- trim index 将原 `has_null_marks` 字节数组定义为 `pack_marks`：bit 0 为 NULL、bit 1 为 low、bit 2 为 high、bit 3..7 在 V1 中必须为 0。
- `TrimMinMaxColumnMeta` 不保存任何 per-pack flags；low/high flags 与 min/max 一起位于 trim index payload。
- trim min/max 使用独立 subfile，不扩展普通 `.idx`。
- `TrimMinMaxIndex` 采用组合而非继承；通用 `MinMaxIndex` 产生原始结果，trim wrapper/Operator 负责方向修正。
- 第一版只写 DMFile V3 / MetaV2。
- 第一版对白名单 equality、IN、bounded range 和边界位于 `E` 内的单边 range 使用 trim；其他谓词 fallback。
- low/high flags 用于判断 trimmed value 是必定匹配还是必定不匹配，并保守修正 `None` 和 `All`。
- 老文件不 backfill，通过自然重写逐步覆盖。
