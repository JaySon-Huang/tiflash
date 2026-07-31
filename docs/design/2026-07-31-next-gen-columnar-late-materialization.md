# Late Materialization for the Next-Generation Columnar Read Path

Purpose: define a production-safe late-materialization architecture for the
TiFlash columnar read path enabled by `ENABLE_NEXT_GEN_COLUMNAR`. The first
delivered operator is Selection with `keep_order = false`. The storage
abstractions must remain usable by future TopN and Join late materialization,
and production readiness requires the optimization to work when a snapshot
contains memtables or unconverted L0 row sources.

Date: 2026-07-31

## Summary

This proposal introduces a snapshot-scoped row identity and a staged
materialization protocol across TiFlash, `tiflash-columnar-hub`, and
`cloud-storage-engine`.

The core decisions are:

1. A physical row is identified by a stable `RowLocator`:

   ```text
   RowLocator = (SourceId, source-local row ordinal)
   ```

2. A Region snapshot owns an immutable `SourceCatalog`. A scan unit, which is
   either a whole Region or one independent bucket range inside a Region, owns
   a `RowSet`. The `RowSet` stores one selection mask per source.
3. MVCC is evaluated by globally merging handle/version/tombstone records from
   all relevant sources. The result is written back to the per-source masks by
   `RowLocator`.
4. For Selection, only predicate-dependent columns are materialized after
   MVCC. TiFlash evaluates the predicate with the existing authoritative
   expression engine and returns a filter mask to the Rust reader. The filter
   mask is intersected with the MVCC `RowSet`.
5. Remaining columns are read in source/file/pack order. This deliberately
   abandons handle order and maximizes sequential IO. It is enabled only when
   `keep_order = false`.
6. The public abstraction is `RowSet` plus `GatherPlan`, not a
   Selection-specific bitmap. A bitmap is the optimized representation for the
   one-row-to-at-most-one-row Selection case. TopN and Join additionally need
   logical output positions, permutations, duplication, and null-side
   semantics.
7. The preview phase may enable the optimization only for pure-columnar
   snapshots, but the feature must not reach general availability while the
   presence of a memtable or unconverted L0 disables it. Row sources participate
   in the same MVCC and Selection flow through source-aware row locators and a
   row-format-specific materializer.
8. Runtime statistics are reported as structured stages with explicit
   aggregation semantics for rows, bytes, columns, worker time, and wall time.
   Existing flat `ColumnarScanContext` fields remain during a compatibility
   period.
9. A `keep_order = false` scan without Selection may also use a
   visibility-first path: build the MVCC RowSet, then read requested payload
   columns in physical order. This mode is independently gated and costed. It
   must not unconditionally replace the existing pack-clean path, which can
   avoid per-row handle/version work for clean packs.
10. The first release does not introduce a general
    `ColumnarMaterializeOp`. It implements the adjacent TableScan-to-Selection
    token/filter protocol and reserves RowSet/GatherPlan materialization
    interfaces for later operator-spanning TopN and Join designs.

The initial production scope covers normal TableScan with
`keep_order = false`. TableScan plus pushed-down Selection is the first rollout
path. A projection-only scan without Selection is a separately controlled mode
using the same visibility and payload materialization contracts. ANN, FTS,
TopN, Join, and complex generated-column predicates are outside the initial
implementation, but the storage contracts introduced here must not prevent
their later implementation.

## Context

### Current request path

With `ENABLE_NEXT_GEN_COLUMNAR`, the read path is:

```text
TiDB DAG
  -> TiFlash PhysicalTableScan / StorageDisaggregatedColumnar
     -> tiflash-columnar-hub FFI
        -> cloud-storage-engine SnapAccess
           -> ColumnarReader tree
              -> complete materialized Block
     -> TiFlash exact Selection
```

Relevant current ownership is:

- TiDB selects predicates for late materialization and writes them into
  `pushed_down_filter_conditions`.
- TiFlash builds the columnar reader and executes the authoritative expression
  semantics.
- `tiflash-columnar-hub` owns the C ABI and the lifetime of the Rust reader.
- `cloud-storage-engine` owns snapshots, source files, MVCC merge, pack IO, row
  decoding, and block serialization.

The TiDB setting `cse.columnar-store-type = "columnar"` changes how columnar
replica availability is managed. It does not create a separate planner
executor family. Requests still use TiFlash TableScan, Selection, TopN, and
Join protobuf executors.

### Verified current behavior

#### Snapshot and source ordering

`tiflash-columnar-hub` requests a snapshot from the TiKV leader and constructs
a `SnapAccess`. Bucket readers for the same Region, epoch, `start_ts`, table
range, and preparation mode can share the same `SnapAccess`.

The snapshot contains a fixed set of sources for its lifetime:

- columnar L0, L1, and L2 files
- memtables
- unconverted L0 row tables
- blob tables referenced by row values

Columnar source ordering is already deterministic:

- L0 and L1 files are sorted by descending snapshot version
- L2 files are sorted by smallest key

The immutable snapshot lifetime is sufficient to assign stable query-local
source identities. Compaction may continue in the storage engine, but it must
not mutate the sources referenced by the captured `SnapAccess`.

Relevant code:

- `contrib/tiflash-columnar-hub/hub-runtime/src/cloud_helper.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/read.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/columnar.rs`

#### Eager projection before MVCC

`ColumnarTableReader` currently owns:

- one handle reader
- one version reader
- one reader for every projected column

Its `read` method reads all of them in lockstep. `ColumnarMergeReader` then
merges complete blocks by `(handle ASC, version DESC)`, and
`ColumnarMvccReader` filters those complete blocks.

Consequences:

- wide payload columns are read and decoded before the system knows whether
  the version is visible
- source identity and source-local row position are lost when
  `ColumnarMergeReader` appends rows to its output block
- after MVCC, the reader cannot return to physical file order without
  reconstructing row identity

Relevant code:

- `components/kvengine/src/table/columnar/reader.rs`
  - `ColumnarReader`
  - `ColumnarTableReader`
  - `ColumnarMergeReader`
  - `ColumnarMvccReader`

#### Row sources are merged and decoded eagerly

`SnapAccess::collect_column_row_readers` collects WRITE CF iterators from
memtables and unconverted L0 row tables. `RowMvccReader` merges those row
iterators, applies MVCC within the row-source stream, and decodes the selected
row into a columnar block. That block then participates in the outer merge with
columnar files.

`table::Value` is a short-lived view and becomes invalid after the iterator
advances. Row values may be inline or represented by a `BlobRef`.

This means production support for row sources cannot be implemented by keeping
a borrowed `Value` in a bitmap. The design needs either:

- a stable source locator that can replay the immutable source iterator, or
- an owned, bounded copy of the row value or Blob reference

This proposal uses stable source locators plus bounded per-batch ownership
during materialization.

Relevant code:

- `components/kvengine/src/read.rs::collect_column_row_readers`
- `components/kvengine/src/table/columnar/reader.rs::RowMvccReader`
- `components/kvengine/src/table/columnar/reader.rs::ColumnarRowTableReader`
- `components/kvengine/src/table/table.rs::Value`

#### Pushed filters are rough checks in Rust

The current Rust `FilterOperator` is a pack-level rough check. It supports only
a subset of types and expressions and intentionally disables cases whose exact
semantics require TiDB collation, timezone, or other expression behavior.

TiFlash therefore merges pushed-down filters with normal filter conditions and
evaluates them again after all columns have been deserialized.

This is a correctness requirement, not temporary duplicate work. The initial
late-materialization implementation must continue to use TiFlash as the
authoritative row-level predicate evaluator.

Relevant code:

- `components/kvengine/src/table/columnar/filter.rs`
- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`

#### Current FFI is single-stage

The current FFI exposes:

```text
fn_get_columnar_reader
fn_read_block
fn_read_handle
fn_read_version
fn_read_column
fn_physical_table_id
fn_columnar_scan_stats
```

`fn_read_block` produces a complete Rust block. TiFlash then retrieves and
deserializes every output column. There is no concept of:

- an MVCC `RowSet`
- early and deferred column projections
- a stable early-batch token
- returning an exact Selection filter to the Rust reader
- gathering deferred columns in physical source order

Relevant code:

- `contrib/tiflash-columnar-hub/hub-runtime/ffi/src/RaftStoreProxyFFI/ProxyFFI.h`
- `contrib/tiflash-columnar-hub/hub-runtime/src/columnar_impls.rs`
- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`

#### Existing DeltaMerge precedent

DeltaMerge already supports this high-level sequence:

1. build an MVCC bitmap
2. read filter columns
3. execute the pushed predicate
4. intersect the predicate filter with the MVCC bitmap
5. read remaining columns with the resulting bitmap

However, the DeltaMerge bitmap is indexed by a Segment-wide row ordinal and
its filter-column and payload streams are naturally aligned. The
next-generation columnar path merges independent sources and currently loses
source identity. DeltaMerge validates the algorithm, but its concrete stream
interfaces cannot be reused unchanged.

Relevant code:

- `dbms/src/Storages/DeltaMerge/VersionChain/MVCCBitmapFilter.cpp`
- `dbms/src/Storages/DeltaMerge/LateMaterializationBlockInputStream.cpp`
- `dbms/src/Storages/DeltaMerge/File/DMFileReader.cpp`

### Problem statement

The current columnar read path pays payload IO, decoding, serialization, FFI,
deserialization, and memory-copy costs for rows that are later removed by MVCC
or Selection.

The problem is most visible when:

- the table is wide
- the filter references few columns
- the predicate is selective
- the snapshot has multiple overlapping versions
- columnar data is remote or cold

A production solution must preserve exact snapshot and expression semantics,
must not depend on output handle order when `keep_order = false`, and must keep
working when recent writes are still in memtables or unconverted L0.

### Constraints and decision drivers

- Snapshot isolation and tombstone semantics must be identical to the eager
  reader.
- `keep_order = true` is never eligible for this design.
- A snapshot includes both columnar and row-format sources.
- Row format is not column-separable. It cannot provide the same physical
  column IO reduction as a columnar file.
- TiFlash remains the source of truth for exact predicate semantics.
- The feature crosses C++, Rust, FFI, tipb, and TiDB runtime statistics.
- Preview rollout may be narrower than production readiness, but source type
  alone must not disable the GA feature.
- Memory consumption must remain bounded and must participate in query memory
  tracking.
- Existing eager reading remains the correctness fallback for unsupported
  operators, excessive RowSet memory, and feature rollback.

## Terminology

| Term | Meaning |
| --- | --- |
| Region snapshot | One immutable `SnapAccess` identified by Region, epoch, `start_ts`, and table scope |
| Physical source | One immutable source inside the snapshot, such as a columnar file, memtable, or unconverted L0 table |
| SourceCatalog | Snapshot-scoped ordered catalog assigning a stable `SourceId` and source priority |
| Scan unit | One independently executable Region or Region bucket handle range |
| RowLocator | `(SourceId, source-local row ordinal)` within one Region snapshot |
| RowSet | Rows retained by MVCC and later operators, partitioned by source |
| Early columns | Columns required before the materialization boundary, initially Selection predicate dependencies |
| Deferred columns | Requested columns not needed before the materialization boundary |
| Batch token | Opaque identifier mapping a C++ filter result to the exact Rust-side early batch |
| GatherPlan | Physical read order plus an optional scatter mapping to logical output positions |
| Materialization source | A source-specific strategy for reading early and deferred data |

## Goals

1. For eligible `keep_order = false` Selection queries, avoid reading
   columnar-file payload columns for rows rejected by MVCC or the pushed
   predicate.
2. Produce exactly the same output row multiset and errors as the eager path.
3. Read deferred columnar data in source/file/pack order and skip packs with no
   selected rows.
4. Preserve a stable row identity through source merge, MVCC, Selection, and
   final materialization.
5. Keep the underlying storage interfaces suitable for TopN and Join, where a
   bitmap alone is insufficient.
6. Report stage-specific time, row, byte, column, source, and pack information
   to TiDB with defined aggregation semantics.
7. Allow safe preview rollout on pure-columnar snapshots.
8. Before GA, execute the optimized path correctly when the same snapshot also
   contains memtables, unconverted L0 row tables, and Blob references.
9. Bound RowSet and row-value staging memory, expose fallback reasons, and
   retain an online rollback switch.
10. For eligible projection-only scans without Selection, allow MVCC visibility
    construction and payload materialization to be decoupled when that is
    cheaper than the eager or pack-clean path.

## Non-goals

- Implementing late materialization through TopN or Join in the first release.
- Supporting `keep_order = true`.
- Reimplementing TiFlash expression semantics in Rust.
- Making row-format sources column-separable.
- Guaranteeing a performance win for every eligible query.
- Replacing the current pack-level rough-check implementation.
- Enabling ANN or FTS in the first release.
- Changing SQL ordering semantics.
- Persisting RowSets across queries or snapshots.
- Sharing a mutable RowSet between Region buckets in the first implementation.
- Replacing pack-clean reads with visibility-first reads when pack-clean is
  cheaper.
- Introducing a general-purpose `ColumnarMaterializeOp` in the first release.

## Architecture overview

```text
Region SnapAccess
  |
  +-- SourceCatalog
      +-- Source 0: columnar file
      +-- Source 1: columnar file
      +-- Source 2: memtable
      +-- Source 3: unconverted L0
      |
      +-- ScanUnit A: [bucket_start, bucket_end)
          |
          +-- Stage 1: key/version cursors from all sources
          |     -> global MVCC merge
          |     -> RowSet[source_id][source-local ordinal]
          |
          +-- Stage 2: materialize early columns in physical order
          |     -> EarlyBatch + BatchToken
          |     -> TiFlash exact predicate
          |     -> retain(BatchToken, filter)
          |     (omitted for a projection-only scan)
          |
          +-- Stage 3: build GatherPlan from retained RowSet
                -> materialize deferred columns
                -> output Block
```

Region buckets share the immutable `SourceCatalog`, but each bucket owns an
independent RowSet:

```text
Region snapshot
  SourceCatalog
    Source A
    Source B
    Source C

  Bucket [h0, h1)
    RowSet[A]
    RowSet[B]
    RowSet[C]

  Bucket [h1, h2)
    RowSet[A]
    RowSet[B]
    RowSet[C]
```

A source can overlap multiple buckets. Conceptually it has a mask in every
overlapping scan unit. The physical representation may store only the
source-row span or pack span relevant to the bucket instead of allocating a
full-file dense bitmap for each bucket.

## Detailed design

### 1. Eligibility and maturity levels

The implementation has two maturity levels.

#### Preview eligibility

The preview path may require:

- `keep_order = false`
- normal TableScan with non-empty `pushed_down_filter_conditions`
- no ANN or FTS
- no unsupported generated-column predicate
- estimated RowSet memory below the configured limit
- no memtable or unconverted L0 row source

If these conditions are not met, the reader uses the existing eager path.

#### GA eligibility

GA removes the pure-columnar restriction. The presence of any of these must
not disable the feature:

- memtable WRITE CF data
- unconverted L0 WRITE CF data
- inline row values
- row values represented by `BlobRef`

ANN, FTS, TopN, Join, and unsupported expression dependencies may remain
ineligible for the Selection GA milestone. The difference is important:

```text
Operator not supported yet       -> allowed GA fallback
Snapshot contains recent writes  -> not an allowed GA fallback
```

The implementation must expose a fallback reason counter so that canary and GA
reviews can verify that row-source presence is no longer a fallback cause.

#### Projection-only visibility-first mode

A normal TableScan with no pushed-down Selection can still separate visibility
from payload reading:

```text
prepare_visibility()
  -> build MVCC RowSet
  -> build GatherPlan in physical source order
  -> materialize all requested payload columns for the MVCC RowSet
```

In this mode there is no exact-predicate phase:

```text
final RowSet = MVCC RowSet
```

The mode is correct whenever `keep_order = false`, but correctness eligibility
does not imply that it is profitable. In particular, the current pack-clean
path can skip per-row handle/version processing for a clean pack and directly
read its payload. Replacing it with mandatory MVCC RowSet construction can
regress CPU, IO, and time to first row.

The reader therefore chooses among these paths before output begins:

| Condition | Preferred candidate |
| --- | --- |
| Clean columnar packs eligible for pack-clean | Existing pack-clean/eager path |
| Overlapping versions or deletes, wide payload, unordered output | Visibility-first RowSet path |
| Unsupported operator or excessive RowSet memory | Existing eager path |

This is initially an independently controlled experimental mode. Benchmarks
and canary data may later supply a cost model, but the behavioral contract does
not require one particular heuristic. At GA, memtables or unconverted L0
sources must not make this mode incorrect or force a source-type-only fallback;
their estimated replay and decode cost may still make the eager path cheaper.

### 2. SourceCatalog

`SourceCatalog` is created from one immutable Region snapshot before any MVCC
read begins.

Each entry contains at least:

```text
SourceDescriptor {
    source_id
    source_kind
    physical_table_id
    stable_priority
    estimated_rows
    key_range
    source-specific metadata
}
```

`source_kind` initially includes:

```text
COLUMNAR_FILE
MEMTABLE
UNCONVERTED_L0
```

For columnar files, source-specific metadata includes file identity, table
metadata, pack row offsets, and encryption metadata.

For row sources, it includes enough immutable state to reopen an iterator from
the captured snapshot and resolve Blob references.

`stable_priority` must reuse the current LSM/source precedence. It must not
invent a new precedence from pointer order. The priority is used only when two
sources expose the same `(handle, version)`. Such duplicate records are
expected to be semantically equivalent, but choosing one deterministically is
required for stable row location and reproducible tests.

A RowSet is invalid if any of these change:

- Region ID or epoch
- `start_ts`
- physical table scope
- SourceCatalog identity or ordering
- scan-unit handle range

Reader retry with a newly requested snapshot creates a new SourceCatalog and
discards the old RowSet.

### 3. RowLocator and RowSet

#### RowLocator

The initial locator is:

```text
struct RowLocator {
    SourceId source_id;
    UInt64 source_row_ordinal;
}
```

The ordinal is the deterministic position of one physical MVCC record in that
source's forward iteration order. It counts versions, not logical handles.

For a scan unit, the implementation may store an ordinal relative to the
source span intersecting that scan unit. The base ordinal then belongs to the
source mask metadata.

The global merge must carry RowLocator alongside handle, version, and
tombstone state. Appending only data values to a merged Block is insufficient.

#### RowSet

Conceptually:

```text
RowSet {
    snapshot_identity
    scan_unit_range
    per_source_masks: Vec<SourceMask>
}
```

The Selection implementation needs only membership. `SourceMask` can therefore
use:

- a dense bitset for dense selections
- a sparse or run-based representation for sparse selections
- pack-aligned chunks for columnar files

The public RowSet API must not expose one concrete bitmap type.

Required operations:

```text
set(locator)
clear(locator)
retain_batch(batch_token, filter)
selected_rows(source_id)
selected_count()
estimate_memory()
build_gather_plan(order)
```

The MVCC mask and Selection mask do not need to coexist for the full query.
Selection can clear the MVCC RowSet in place, using only a temporary batch
filter.

#### GatherPlan

`GatherPlan` separates logical row selection from physical IO order:

```text
GatherPlan {
    physical_runs
    optional_scatter_positions
}
```

For Selection with `keep_order = false`, `optional_scatter_positions` is
absent.

For future TopN, the physical runs are sorted for IO, while scatter positions
restore TopN rank.

For future Join, the gather step may deduplicate physical reads, while scatter
positions reproduce row multiplicity and null-side output.

### 4. Scan-unit and bucket ownership

A scan unit is identified by:

```text
(region_id, region_epoch, start_ts, physical_table_id, handle_range)
```

It owns:

- one Region snapshot reference
- one shared immutable SourceCatalog reference
- one independent RowSet
- one staged reader state machine
- one set of runtime statistics

The initial implementation must not make buckets write to one Region-wide
mutable bitmap. Independent RowSets avoid:

- atomic bitmap writes
- ambiguous memory ownership
- retry cleanup races
- cross-bucket statistics attribution
- accidental duplicate output

Bucket ranges must be normalized half-open handle ranges:

```text
[start_handle, end_handle)
```

All versions of one handle must belong to the same scan unit. Range
normalization must merge or reject overlaps so that one physical row is not
returned twice.

### 5. MVCC RowSet construction

#### Lightweight source cursors

Every materialization source exposes a cursor producing:

```text
(handle, version, tombstone, RowLocator)
```

The cursor does not materialize user payload columns.

For a columnar file:

- read handle packs
- read nullable version packs
- use the version null bit as tombstone state
- advance the file-local ordinal with every physical version

For row sources:

- iterate WRITE CF keys and versions
- obtain version and tombstone metadata from `table::Value`
- do not decode row columns
- do not fetch a Blob value merely to construct MVCC state
- retain the winning child source identity and source-local ordinal through
  any internal row-source merge

The current `RowMvccReader` merges row sources before the outer columnar merge.
The optimized implementation may keep that shape for efficiency, but its
output must become:

```text
(handle, visible-row-source version, tombstone, RowLocator)
```

instead of a fully decoded row block. Its source-aware merge iterator must
preserve the winning child `SourceId` and ordinal.

Alternatively, each row source may participate directly in the outer merge.
This is semantically simpler but can create a larger merge heap. The preferred
implementation is the source-aware internal row merge because it preserves
the current reader layering and avoids decoding invisible row versions.

#### Global MVCC merge

All columnar cursors and the source-aware row cursor participate in the global
merge ordered by:

```text
(handle ASC, version DESC, stable_priority ASC)
```

For each handle:

1. skip versions greater than `read_ts`
2. select the first remaining version
3. if it is a tombstone, select no row for the handle
4. otherwise call `RowSet::set(locator)`
5. skip the remaining versions of that handle

The implementation should perform a k-way merge. It must not collect and sort
the entire handle/version array in memory.

#### Pack rough check

Predicate rough check and MVCC range pruning must be treated as different
masks.

Key-range pruning may exclude a pack from the MVCC cursor only when the pack
cannot contain a handle in the scan-unit range.

Predicate rough check must not remove handle/version records before MVCC. A
newer visible version that cannot satisfy the predicate still shadows older
versions of the same handle in other sources. Removing it before MVCC could
incorrectly expose and return an older version.

The safe order is:

1. build the MVCC RowSet from all versions in the requested handle range
2. evaluate the conservative predicate rough check
3. for a definitely-not-match pack, clear its already-selected MVCC bits and
   skip reading its predicate columns
4. retain unknown packs and let TiFlash evaluate the exact predicate

The new reader should therefore keep separate concepts such as:

```text
mvcc_range_pack_mask
predicate_rough_check_result
```

A pack skipped after MVCC still occupies its original source-ordinal range.
Later sources and columns must not renumber rows because the pack was skipped.

### 6. Column classification

TiFlash derives two projections:

```text
early_columns
deferred_columns
```

For the initial Selection implementation:

```text
early_columns =
    dependency closure of pushed_down_filter_conditions
    + handle columns required by the expression
    + columns required by casts, timezone conversion, and supported generated expressions

deferred_columns =
    requested output columns - columns reusable from early_columns
```

Special columns read for MVCC are not automatically returned to TiFlash. If
the query requests the handle or version, the reader should reuse the already
read data when doing so is cheaper than rereading it.

The planner-selected early predicate is:

```text
early_predicate = table_scan.pushed_down_filter_conditions
```

The post-materialization predicate is:

```text
residual_predicate = Selection conditions not evaluated as early_predicate
```

The implementation must explicitly split these sets. It must not accidentally
evaluate all Selection expressions early and defeat TiDB's cost decision. It
must also avoid treating the Rust rough-check result as exact row filtering.

### 7. Staged Selection flow

The staged flow for one scan unit is:

```text
prepare_visibility()
  -> build MVCC RowSet

while early rows remain:
  batch = materialize_early(RowSet, early_columns, PHYSICAL_ORDER)
  filter = TiFlashExactPredicate(batch)
  retain(batch.token, filter)

finish_early_phase()

while output rows remain:
  block = materialize_deferred(RowSet, deferred_columns, PHYSICAL_ORDER)
  apply residual predicate if present
  emit block
```

An early batch contains:

- the early column block
- row count
- physical table ID
- an opaque BatchToken

The BatchToken is owned by the Rust reader and maps each batch position to the
corresponding RowLocator. C++ must not decode or manufacture RowLocators.

`retain` validates:

- token belongs to this reader and current stage
- filter length equals batch row count
- token has not already been consumed

After validation, it clears RowSet entries for false positions and releases
batch-specific state.

#### Adjacent and operator-spanning row identity

The first Selection implementation keeps RowLocators opaque to C++. An
adjacent TableScan-to-Selection handoff uses `EarlyBatch + BatchToken`, and
Selection returns only its filter mask. This avoids serializing and
deserializing a physical locator column on the initial hot path.

A future operator-spanning implementation may expose a hidden `RowRefColumn`
whose values refer to RowLocators. Every such column must be bound to the
reader's pinned snapshot through a `MaterializerHandle`; neither C++ nor another
reader may reinterpret the locator independently.

### 8. Materializing columnar-file sources

After MVCC, a columnar file is read in:

```text
SourceCatalog order
  -> pack order
     -> row order inside pack
```

For early columns:

- skip packs with no MVCC-selected rows
- load the early-column pack once
- gather selected rows into an early batch

After Selection:

- skip packs with no surviving rows
- load only deferred-column packs that contain surviving rows
- gather selected rows

This is the best available physical IO continuity under `keep_order = false`.
A compressed pack with one selected row may still require reading and
decompressing the whole pack. The expected saving is therefore pack-granular
for IO and row-granular for decoded/serialized data.

`ColumnarColumnReader::set_row_idx` already provides a lower-level positioning
mechanism, but the public reader API must be extended to express selected
ranges or runs rather than issuing one random seek per row.

The public storage seam should be equivalent to:

```text
materialize_by_rowset(snapshot, rowset, column_ids, physical_order)
materialize_by_gather_plan(snapshot, gather_plan, column_ids)
```

The concrete FFI may keep the RowSet inside the reader and expose incremental
`read_deferred_block` calls. The contract matters more than transporting an
explicit bitmap or RowLocator array across the ABI.

#### Interaction with pack-clean

Pack-clean and delayed materialization are complementary fast paths:

- pack-clean proves that a pack does not require row-level MVCC work
- delayed materialization uses an MVCC RowSet to avoid payload reads for
  invisible or predicate-rejected rows

For Selection, predicate selectivity can justify RowSet construction even when
some packs are clean. For projection-only scans, clean-pack eligibility is a
strong reason to preserve the current pack-clean read. The path selector must
record which path was chosen and must never apply predicate rough check before
MVCC merely to make RowSet construction cheaper.

### 9. Materializing memtable and unconverted L0 row sources

Row-format sources require a different materialization strategy because all
columns are encoded in one row value.

Their presence must not disable the query-wide optimization at GA.

#### Source-aware replay

After the MVCC RowSet is built, `RowSourceMaterializer` reopens an iterator from
the same immutable snapshot source and scan-unit range.

It advances in source order while tracking the same source-local ordinal used
during MVCC. It consults the source mask before decoding a value:

- mask bit is zero: advance without decoding the row
- mask bit is one: stage the row for the current early batch

The replay path validates the expected key/version at selected positions in
debug builds and tests. A mismatch means that source identity or ordinal
stability was violated and is a correctness error.

#### Bounded DeferredRowValue

`table::Value` cannot escape the iterator. For each selected row in the
current early batch, the materializer creates an owned bounded representation:

```text
DeferredRowValue =
    InlineRowValue(owned encoded row bytes)
    | BlobRowValue(BlobRef, optional fetched/decrypted row bytes)
    | Tombstone
```

Only the current early batch is retained. The memory is charged to the query
memory tracker and bounded by both row count and byte size.

Behavior depends on early-column dependencies:

- Predicate needs only handle-derived columns:
  - do not fetch Blob payload before the exact filter
  - fetch/decode payload only for passing rows
- Predicate needs a value column:
  - fetch Blob payload if necessary
  - decode only early columns
  - keep the fetched/decrypted row bytes until the filter returns
  - decode deferred columns only for passing rows
- Inline row:
  - copy the encoded row bytes into the bounded batch because the iterator view
    is short-lived
  - decode only early columns before the filter
  - decode deferred columns from the same owned bytes only for passing rows

Rows rejected by the exact predicate release their `DeferredRowValue`
immediately.

This does not make row format column-separable. Depending on the underlying
table iterator, reading an inline row may still load its complete encoded
value. The optimization remains useful because it can avoid:

- decoding all row columns before MVCC
- fetching Blob payload when the predicate needs only handle data
- decoding deferred columns for predicate failures
- serializing and transferring deferred columns through FFI
- materializing rejected columns in C++

If a row value is too large for the configured batch byte limit, it is
processed as a single-row batch instead of disabling late materialization for
the whole snapshot.

#### Output coordination

After C++ returns the filter for a row-source early batch:

1. discard failed `DeferredRowValue` entries
2. decode deferred columns for passing entries
3. append the completed rows to a bounded output queue
4. release raw row storage once the completed block owns its columns

Because `keep_order = false`, completed row-source blocks and columnar-file
blocks may be emitted in any source order chosen by the scan unit.

This design avoids retaining all visible row values until the full predicate
phase finishes.

### 10. FFI and reader state

The Rust reader becomes an explicit state machine:

```text
CREATED
  -> PREPARING_VISIBILITY
  -> READING_EARLY
  -> READING_DEFERRED
  -> DRAINED
  -> FAILED
```

Invalid transitions return a reader error and do not produce partial blocks.

The exact ABI can be finalized in an implementation sub-proposal, but it must
represent these operations:

```text
prepare_visibility(reader)
read_early_block(reader, limit)
read_early_column(reader, column_id)
apply_early_filter(reader, batch_token, filter_bytes)
finish_early_phase(reader)
read_deferred_block(reader, limit)
read_deferred_column(reader, column_id)
read_runtime_stats(reader)
```

The creation request carries a serialized read plan containing:

- mode: eager or Selection late materialization
- early column IDs
- deferred column IDs
- scan-unit ranges
- expression metadata needed for rough check

Variable-size plans and stage statistics should cross the FFI as serialized
protobuf or owned Rust buffers. They should not continuously grow the fixed C
struct ABI.

The existing eager functions remain available during rollout. A reader is
created in one mode and cannot switch snapshots or source catalogs mid-read.

The first release does not need a standalone C++ `ColumnarMaterializeOp`.
Selection is coordinated inside the columnar source using the state machine
above. The RowSet/GatherPlan seam is nevertheless explicit so that later TopN
and Join designs can add an operator boundary without replacing the storage
protocol.

### 11. TiFlash execution integration

TiFlash currently executes exact pushed filters after complete column
deserialization. The late-materialization path moves only the selected early
predicate into the staged source:

```text
RNColumnar source
  -> deserialize early columns
  -> existing TiFlash expression actions
  -> produce FilterPtr
  -> FFI retain(batch token, filter)
  -> deserialize deferred output
  -> residual filter
```

The expression implementation, timezone normalization, casts, NULL behavior,
collation behavior, and error behavior must be reused rather than duplicated.

Both BlockInputStream and Pipeline execution modes need the same semantic
state machine. They should share one late-materialization coordinator instead
of independently implementing filter/token rules.

The coordinator owns:

- the Rust reader handle
- early and deferred headers
- expression actions for the early predicate
- batch-token lifecycle
- C++-side stage statistics
- cancellation and memory tracking

### 12. Extensibility to TopN

TopN needs:

- order-expression dependency columns as early columns
- a bounded candidate heap
- RowLocators for surviving candidates
- logical rank

After TopN candidate selection:

1. build a GatherPlan sorted by physical source position
2. materialize deferred columns in physical order
3. scatter rows back to TopN rank
4. emit in TopN order

This is why the storage contract includes optional scatter positions and does
not define RowSet as only one output bitmap.

The initial TopN extension is local to one task and pinned snapshot. Deferred
columns must be materialized before candidates cross an Exchange boundary.

TopN implementation and optimizer integration require a follow-up design.

### 13. Extensibility to Join

Join can produce:

- zero output rows for one input row
- one output row
- multiple output rows
- an unmatched row with a null side

Join therefore needs an ordered logical output containing:

```text
(left RowLocator or NULL, right RowLocator or NULL)
```

The gather step can deduplicate physical locators before reading deferred
columns and then scatter values into all logical join output positions.

This storage-level late materialization is distinct from the existing JoinV2
late-materialization optimization. JoinV2 delays copying columns that are
already present in in-memory Blocks; this proposal delays reading columns from
snapshot sources. A future Join may compose both optimizations, but neither is
a replacement for the other.

The initial Selection design does not implement this pair representation, but
the following contracts are deliberately reusable:

- snapshot-scoped SourceCatalog
- stable RowLocator
- source-specific materializers
- physical GatherPlan
- scatter positions
- operator-owned materialization statistics

Join implementation and planner rules require a follow-up design.

## Correctness and compatibility invariants

### Snapshot visibility

For each handle `h`, define the visible version at timestamp `t` as the first
record in `(version DESC, stable_priority ASC)` order satisfying
`version <= t`.

If that record is a tombstone, `h` has no visible row.

The MVCC RowSet contains exactly the RowLocator of every visible non-tombstone
record in the requested range.

The nullable version bit is part of the MVCC input and must not be dropped.

### Selection equivalence

Let:

```text
M(r) = row r is selected by MVCC
P(r) = TiFlash exact predicate evaluates to true for r
```

The final RowSet contains:

```text
S(r) = M(r) AND P(r)
```

The eager and delayed paths therefore produce the same row multiset:

```text
{ full_row(r) | S(r) }
```

The delayed path enumerates that set in physical source order instead of
handle order. This is valid only because `keep_order = false`.

### Projection-only equivalence

For a scan without Selection:

```text
S(r) = M(r)
```

The delayed path therefore returns exactly the same visible row multiset as the
eager path. It may enumerate that multiset in source/file/pack order because
`keep_order = false`. Pack-clean and visibility-first execution are alternative
physical proofs of the same MVCC-visible set; choosing between them cannot
change SQL-visible results.

### Column alignment

For every source:

- handle, version, early columns, and deferred columns use the same physical
  row numbering
- pack skipping does not renumber rows
- default and missing columns are reconstructed with the same schema rules as
  the eager path
- common-handle-derived columns use the same decoding behavior

### Row-source equivalence

For row sources:

- source replay uses the exact snapshot source captured during MVCC
- source-local iteration order is deterministic
- selected row key/version matches the MVCC cursor's locator
- Blob resolution and encryption use the same code as
  `ColumnarRowTableReader`
- early and deferred projection errors are identical to eager decoding for
  rows that are semantically required
- invisible or predicate-rejected rows must not introduce schema errors merely
  because their deferred columns would fail to decode

The last invariant is important: the existing `RowMvccReader` was introduced
partly to avoid decoding invisible historical values that may be incompatible
with the current schema.

### Range and bucket behavior

- ranges are normalized and non-overlapping
- bucket boundaries do not split versions of one handle
- each bucket has an independent RowSet
- a physical source overlapping multiple buckets can be read by each bucket,
  but the row ranges are disjoint

### Duplicate `(handle, version)`

Exact duplicate MVCC records from different sources must resolve using the
current source precedence exposed as `stable_priority`.

Tests must verify:

- only one RowLocator is selected
- eager and delayed values are equivalent
- the selected locator is deterministic

### Snapshot locality and Exchange boundary

`SourceId`, source-local ordinals, RowSets, RowRefColumns, and
MaterializerHandles are meaningful only within the Region snapshot and reader
lifetime that created them.

They must not cross an Exchange boundary. The safe execution order is:

```text
local scan
  -> local Selection / future local TopN or Join candidate processing
  -> materialize deferred columns with the pinned MaterializerHandle
  -> Exchange fully materialized rows
```

Sending a physical RowLocator to another TiFlash task and reopening a new
snapshot there is invalid. A future design that needs remote deferred
materialization must define a stable logical identity and reattachment
protocol; it cannot reuse the physical RowLocator contract directly.

### Upgrade and downgrade

- New tipb fields are optional or repeated.
- Old TiDB ignores new stage details.
- New TiDB handles missing stage details from old TiFlash.
- Existing flat `ColumnarScanContext` fields remain populated during the
  compatibility period.
- The feature flag can force the eager reader without changing the query plan.
- No persisted storage format changes are required.

## Runtime statistics and observability

### Ownership

Two related summaries are needed.

#### ColumnarScanContext

This remains responsible for storage facts:

- snapshot acquisition
- source and pack counts
- MVCC key/version reads
- predicate-column reads
- payload-column reads
- physical and logical bytes
- Rust serialization and C++ deserialization

#### LateMaterializationContext

This is attached to `ExecutorExecutionSummary` and is owned by the logical
executor:

- Selection predicate evaluation and retain
- future TopN candidate selection and output reorder
- future Join candidate pairing, gather, and scatter

Executor ownership prevents TopN and Join costs from being incorrectly
reported as only TableScan costs.

### Stage summary

Add a repeated structured summary similar to:

```text
ColumnarStageSummary {
    stage
    read_mode
    source_kind
    task_count
    input_rows
    output_rows
    physical_read_bytes
    decoded_bytes
    serialized_bytes
    column_count
    total_packs
    skipped_packs
    total_worker_time_ns
    max_task_wall_time_ns
}
```

Initial stages:

```text
SNAPSHOT_ACQUIRE
READER_INIT
MVCC_KEY_READ
MVCC_MERGE_FILTER
ROUGH_CHECK
PREDICATE_COLUMN_READ
PREDICATE_EVAL
ROWSET_RETAIN
PAYLOAD_COLUMN_READ
SERIALIZE
FFI_DESERIALIZE
OUTPUT_REORDER
```

`source_kind` distinguishes at least:

```text
ALL
COLUMNAR_FILE
MEMTABLE
UNCONVERTED_L0
```

`read_mode` distinguishes at least:

```text
EAGER
PACK_CLEAN
LATE_MATERIALIZATION_SELECTION
VISIBILITY_FIRST_WITHOUT_FILTER
```

It is a grouping dimension, not a value merged across different modes.

### Aggregation semantics

Every field has one defined merge operation:

| Field | Merge rule |
| --- | --- |
| input/output rows | sum |
| physical/decoded/serialized bytes | sum |
| task count | sum |
| pack counts | sum |
| column count | max for the same stage |
| total worker time | sum |
| maximum task wall time | max |
| feature-enabled flag | logical OR |

`total_worker_time_ns` can exceed query wall time under concurrency.
`max_task_wall_time_ns` is not a full critical-path measurement, but avoids
presenting summed worker time as elapsed latency.

The TiDB display must label them differently.

### TiDB display projection

Structured stage summaries are the authoritative representation. For concise
EXPLAIN ANALYZE and compatibility output, TiFlash/TiDB may derive:

| Display group | Structured stages | Suggested fields |
| --- | --- | --- |
| `mvcc_read` | `MVCC_KEY_READ`, `MVCC_MERGE_FILTER` | time, rows, physical bytes, columns |
| `filter_read` | `ROUGH_CHECK`, `PREDICATE_COLUMN_READ`, `PREDICATE_EVAL`, `ROWSET_RETAIN` | time, rows, physical/decoded bytes, columns |
| `late_read` | `PAYLOAD_COLUMN_READ`, `SERIALIZE`, `FFI_DESERIALIZE` | time, rows, physical/decoded/serialized bytes, columns |

At minimum, the projection also exposes `rows_after_filter` and
`late_materialized_rows`. A projection-only visibility-first scan has no
`filter_read` group and reports `rows_after_filter` equal to the MVCC output
rows.

These display fields are derived views, not independent counters. This avoids
double counting and preserves one merge rule for every underlying value.

### Required row-source metrics

GA review requires visibility into the row-source path:

- row-source physical versions scanned for MVCC
- row-source MVCC-visible rows
- row-source predicate input and output rows
- inline encoded bytes staged
- Blob bytes fetched before predicate
- Blob bytes fetched after predicate
- deferred decode bytes avoided
- peak DeferredRowValue batch bytes
- source replay time

### Fallback reasons

Expose counters for:

```text
KEEP_ORDER
UNSUPPORTED_OPERATOR
ANN_OR_FTS
UNSUPPORTED_EXPRESSION_DEPENDENCY
ROWSET_MEMORY_LIMIT
FEATURE_DISABLED
PROTOCOL_UNSUPPORTED
INTERNAL_SAFETY_FALLBACK
HAS_ROW_SOURCE_PREVIEW_ONLY
PACK_CLEAN_PREFERRED
```

`HAS_ROW_SOURCE_PREVIEW_ONLY` is allowed during preview but must be removed as
a GA fallback reason. `PACK_CLEAN_PREFERRED` records an intentional physical
path choice for projection-only scans rather than a correctness limitation.

### Existing flat fields

The current fields, including MVCC input/output, read block time, serialize
time, prefetch time, rough-check packs, and deserialize time, remain populated
by aggregating the new stage data.

They can be removed only through a separate compatibility decision after all
supported TiDB versions consume structured stages.

## Memory management

### RowSet

Before enabling the delayed path, estimate:

```text
sum(source mask bytes)
+ merge cursor memory
+ batch token memory
+ early block memory
+ row-value staging limit
```

Add a dynamic query setting such as:

```text
columnar_late_materialization_max_rowset_bytes
```

If the estimate exceeds the limit before output begins, use the eager reader
and report `ROWSET_MEMORY_LIMIT`.

Source masks should support pack-aligned or sparse representation so that a
bucket does not always allocate a full dense bitmap for every overlapping
file.

### Row values

Add both row and byte limits:

```text
columnar_late_materialization_row_batch_rows
columnar_late_materialization_row_batch_bytes
```

All owned inline bytes and fetched Blob bytes are charged to the query memory
tracker.

A single row larger than the batch byte limit is processed alone. It is still
subject to the query's normal hard memory limit.

### Output buffering

Row-source completed blocks and columnar deferred blocks use a bounded queue.
The coordinator must not materialize the entire RowSet into C++ memory before
returning the first output block.

The MVCC phase itself remains a pipeline breaker for one scan unit. Bucket
parallelism limits the time-to-first-row impact by allowing independent scan
units to prepare and emit concurrently.

## Failure handling and rollback

### Failure handling

- Snapshot, epoch, lock, and PD errors keep their current retry/error behavior.
- A snapshot retry discards the old reader, SourceCatalog, RowSet, and batch
  tokens.
- Invalid FFI state transition fails the reader.
- Invalid or reused BatchToken fails the reader.
- Filter length mismatch fails the reader.
- Source replay locator mismatch fails the reader.
- Cancellation releases the RowSet, owned row values, Rust buffers, and C++
  blocks.
- No RowSet is reused after a reader error.

The implementation must not silently switch from delayed to eager mode after
it has emitted rows. Fallback is chosen before output, or the query fails and
uses the existing query-level retry behavior.

### Feature controls

At minimum:

```text
columnar_enable_late_materialization
columnar_enable_visibility_first_without_filter
columnar_late_materialization_max_rowset_bytes
columnar_late_materialization_row_batch_rows
columnar_late_materialization_row_batch_bytes
```

The main enable switch is dynamic and rollbackable. Disabling it affects new
readers; it does not mutate active reader state.

The full deployment requires all of these layers to agree:

| Layer | Control | Meaning |
| --- | --- | --- |
| Build | `ENABLE_NEXT_GEN_COLUMNAR` | Compiles the next-generation columnar path |
| TiFlash runtime | `flash.use_columnar` | Selects the columnar storage path |
| TiDB | `cse.columnar-store-type = "columnar"` | Enables the TiDB-side columnar-store mode used by this path |
| Late materialization | `columnar_enable_late_materialization` | Enables staged Selection for new readers |
| Projection-only experiment | `columnar_enable_visibility_first_without_filter` | Allows the no-Selection visibility-first candidate |

The two late-materialization settings do not override the build, TiFlash, or
TiDB controls. Rollout automation and diagnostics should report the effective
state of every layer rather than only the final feature flag.

## Incremental delivery plan

This document is the parent architecture proposal. The implementation is split
into independently reviewable workstreams. TopN and Join will use separate
follow-up proposals because their logical-output contracts are materially
different from Selection.

### Phase A: contracts and differential harness

- Introduce SourceCatalog, RowLocator, RowSet, SourceMask, and GatherPlan.
- Add source identity to columnar merge buffers.
- Add eager-versus-delayed differential test utilities.
- Add feature settings and fallback reason reporting.
- Keep the feature disabled.

Exit criteria:

- RowLocator stability is verified for L0/L1/L2 files and Region buckets.
- Differential tests can compare output multisets and error behavior.

### Phase B: pure-columnar preview

- Build MVCC RowSets from handle/nullable-version columns.
- Add early/deferred projections.
- Add staged FFI and TiFlash exact predicate retain.
- Materialize payload in file/pack order.
- Add structured stage statistics and TiDB parsing.
- Reserve RowSet/GatherPlan-based deferred materialization interfaces; do not
  add a general `ColumnarMaterializeOp`.
- Add the projection-only visibility-first path behind its independent setting
  and retain pack-clean when selected by the path policy.
- Enable only by explicit setting or canary configuration.

Exit criteria:

- correctness matrix passes for int and common handles
- no output-order assertion exists for `keep_order = false`
- memory limit fallback is tested
- canary metrics show expected payload-byte reduction
- projection-only differential tests match the eager path
- clean-pack benchmarks show no forced regression from bypassing pack-clean

### Phase C: row-source support

- Make row-source merge preserve SourceId and source-local ordinal.
- Avoid decoding row values during MVCC.
- Implement immutable source replay guided by RowSet.
- Implement bounded DeferredRowValue batches.
- Reuse Blob resolution, decryption, row-v1/v2 decode, defaults, and schema
  checks from `ColumnarRowTableReader`.
- Add row-source-specific statistics.

Exit criteria:

- memtable presence does not cause fallback
- unconverted L0 presence does not cause fallback
- inline and BlobRef row values pass differential tests
- mixed row and columnar versions of the same handle pass MVCC tests
- row-source staging remains within configured memory bounds

### Phase D: production hardening and GA

- Run mixed-source fault injection and long-running stress tests.
- Validate upgrade/downgrade combinations.
- Tune default RowSet and batch limits.
- Add dashboards and alerts for fallback, bitmap memory, Blob reads, stage
  latency, and payload-byte reduction.
- Remove `HAS_ROW_SOURCE_PREVIEW_ONLY` from accepted fallback reasons.

GA gates:

1. No known correctness difference from the eager path.
2. Both execution engines use the same staged semantics.
3. Mixed memtable, unconverted L0, and columnar snapshots remain optimized.
4. No unbounded query-local row-value retention.
5. Runtime details reach TiDB and preserve old-version compatibility.
6. The online kill switch is verified.
7. Projection-only path selection preserves pack-clean for workloads where it
   is the faster candidate.

### Phase E: follow-up operators

- TopN proposal and implementation using GatherPlan scatter positions.
- Join proposal and implementation using paired nullable RowLocators and
  multiplicity.

These are not Selection GA blockers.

## Validation strategy

### Rust unit tests

#### MVCC matrix

- versions below, equal to, and above `read_ts`
- visible tombstone
- tombstone above `read_ts`
- int handle and common handle
- version chains crossing L0, L1, and L2 columnar files
- version chains crossing columnar file and memtable
- version chains crossing columnar file and unconverted L0
- exact duplicate `(handle, version)` across sources
- newer visible version in a predicate-rough-check-not-match pack shadowing an
  older matching version in another source
- handle at range and bucket boundaries
- empty range and empty source
- multiple physical tables

Each case compares the selected RowLocators and final values with the eager
reader.

#### Row source matrix

- memtable inline row v1 and v2
- unconverted L0 inline row v1 and v2
- BlobRef row
- encrypted BlobRef row
- default and missing columns
- nullable and not-null schema behavior
- common-handle-derived primary-key columns
- invisible historical row with incompatible schema
- predicate rejecting all row-source rows
- predicate retaining all row-source rows
- one row larger than the configured row batch byte limit

#### RowSet and GatherPlan

- dense, sparse, and pack-aligned masks
- in-place retain
- empty and all-match fast paths
- source span base ordinals
- invalid token and length mismatch
- physical order without scatter
- future-facing scatter property tests
- RowLocator and MaterializerHandle rejection after reader/snapshot lifetime

### TiFlash unit tests

- early/residual predicate split
- exact NULL semantics
- timestamp normalization and timezone casts
- string collation behavior
- expression error propagation
- generated-column dependency guard
- BlockInputStream and Pipeline equivalence
- cancellation during MVCC, early read, predicate retain, and payload read
- C++/Rust memory release on every error path

### Differential integration tests

Run each query with:

```text
late materialization disabled
late materialization enabled
```

Compare:

- unordered result multiset
- warnings and errors
- affected executor row counts
- snapshot timestamp behavior

Test shapes:

- projection-only wide table
- projection-only clean L2 data with pack-clean eligible
- projection-only overlapping versions and deletes
- selective single-column predicate
- multi-column predicate
- selectivity near 0%, 50%, and 100%
- partition tables
- multiple Region buckets
- LIMIT without ORDER BY
- aggregation above Selection
- TopN above the materialization boundary while TopN itself remains eager
- hash join consuming a fully materialized scan

### Fault injection

- Region epoch change before snapshot acquisition
- snapshot request retry
- lock error
- source read error during MVCC
- Blob read error during early materialization
- FFI error after early batch creation
- cancellation while C++ holds a BatchToken
- memory limit exceeded before output

### Benchmarks

Measure at least:

- end-to-end latency
- time to first row
- physical read bytes by source kind and stage
- decoded and serialized bytes
- C++ deserialized bytes
- peak RowSet memory
- peak DeferredRowValue memory
- CPU time in MVCC merge, row decode, serialization, and predicate evaluation
- selected physical path and pack-clean eligibility

Workload dimensions:

- number of projected columns
- early/deferred column width
- predicate selectivity
- number of versions per handle
- number and size of columnar files
- cold and warm remote cache
- memtable/unconverted L0 ratio
- inline versus BlobRef row values
- bucket concurrency
- pack-clean ratio

The benchmark report must separately state benefits for columnar and row
sources. A mixed-source query can be a net win even when the row portion saves
CPU and serialization but not inline row-value physical IO.

### Upgrade compatibility

Test:

- new TiDB with old TiFlash
- old TiDB with new TiFlash
- mixed TiFlash versions in one MPP query
- feature disabled during rolling upgrade
- feature enabled only after protocol capability is known

## Impacts

Expected positive impacts:

- lower remote and local payload IO for selective wide-column queries
- lower Rust decode and serialization cost
- lower FFI traffic and C++ deserialization cost
- lower memory-copy cost in global MVCC merge
- explicit stage-level runtime visibility
- reusable row identity for future TopN and Join work

Expected costs:

- MVCC becomes a blocking preparation stage per scan unit
- RowSet consumes query memory
- handle/version columns may be read separately from payload
- row sources may require source replay
- inline row format cannot skip physical bytes by column
- staged FFI and cancellation behavior are more complex

## Risks and mitigations

### RowSet memory exceeds the expected benefit

Mitigations:

- pre-read memory estimation
- adaptive SourceMask representation
- per-query hard limit
- eager fallback before output
- stage and fallback metrics

### Time to first row regresses

Mitigations:

- Region bucket scan units
- parallel visibility preparation
- cost-based TiDB predicate selection
- benchmark and canary thresholds
- retain eager path for non-beneficial cases

### Projection-only mode regresses pack-clean scans

Mitigations:

- keep projection-only visibility-first behind an independent setting
- evaluate pack-clean eligibility before choosing the path
- record the selected path and relevant pack counts
- benchmark clean L2 and overlapping-version workloads separately
- fall back before output begins

### Row source replay loses locator alignment

Mitigations:

- immutable snapshot-owned source descriptors
- source-local ordinal defined over all versions
- key/version validation in tests and debug builds
- fail rather than silently returning a different row

### Row-value staging becomes unbounded

Mitigations:

- retain only the current early batch
- row and byte batch limits
- query memory tracking
- single-row treatment for oversized values
- bounded output queue

### Exact expression semantics drift

Mitigations:

- keep exact evaluation in TiFlash
- reuse existing expression actions and casts
- Rust rough check remains conservative only
- differential expression and error tests

### Duplicate versions select inconsistent sources

Mitigations:

- expose current source precedence as stable priority
- deterministic tie-break
- assert semantic equivalence in tests

### Runtime statistics overstate latency

Mitigations:

- separate summed worker time from max task wall time
- define every merge operation
- avoid displaying worker sum as elapsed time

### Staged FFI leaks state on cancellation

Mitigations:

- explicit reader state machine
- single-use BatchTokens
- RAII cleanup on the C++ side
- Rust ownership of token mappings
- cancellation tests for every stage

## Alternatives considered

### Keep a bitmap indexed by global merge output position

Rejected because merge position is not a physical source position. It cannot
be used to reread independent files in physical order after the merge.

### Use one mutable Region-wide bitmap shared by buckets

Rejected for the initial implementation because it introduces concurrent
writes, retry cleanup, memory ownership, and statistics ambiguity. Buckets
share SourceCatalog but own independent RowSets.

### Evaluate exact predicates in Rust

Rejected because it duplicates TiFlash expression semantics, including
collation, timezone, casts, NULL behavior, and errors. Rust continues to own
only conservative rough check.

### Permanently fall back when row sources exist

Rejected because recent writes make memtables and unconverted L0 normal
production states. A feature that disappears in their presence cannot be
considered generally available.

### Copy all visible row values during MVCC

Rejected because `table::Value` is short-lived and copying all inline or Blob
payloads creates unbounded query memory proportional to visible row data.
Stable locators plus bounded materialization batches provide the required
lifetime without whole-query retention.

### Randomly seek every selected row source value

Rejected as the default because sparse point seeks can destroy IO continuity.
The selected source is replayed sequentially in source order and skips decode
for zero mask bits.

### Always build an MVCC RowSet for projection-only scans

Rejected because clean packs can use pack-clean to avoid per-row
handle/version work. Projection-only visibility-first reading remains a
costed, independently controlled candidate rather than a universal
replacement.

### Reuse DeltaMerge stream classes directly

Rejected because DeltaMerge has one Segment-wide row ordinal and naturally
aligned streams, while the next-generation path has independent snapshot
sources behind an FFI boundary. The algorithm and invariants are reused, not
the concrete stream ownership.

## Proposal and follow-up shape

This document intentionally keeps one parent architecture because the
snapshot identity, RowLocator, RowSet, source materializers, FFI lifecycle,
correctness proof, and statistics model must agree across all implementation
repositories.

Implementation PRs should remain split along the phases above.

TopN and Join require separate follow-up proposals before implementation. They
will reuse this storage foundation but introduce independently reviewable
logical-output and optimizer contracts.

The first Selection and projection-only deliveries intentionally do not add a
general `ColumnarMaterializeOp`. Such an operator is justified only when a
future TopN or Join design needs RowRefColumns to survive across existing
executor boundaries. Even then, materialization must occur before Exchange
unless a separate logical reattachment protocol is designed.

Concrete bitmap encoding thresholds, default memory limits, and batch sizes
are implementation tuning values. They will be selected through benchmarks and
canary data without changing the behavioral contracts in this proposal.
