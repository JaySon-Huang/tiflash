# Stage 3: End-to-End Staged Selection

## Goal

Connect the pure-columnar storage reader to TiFlash expression evaluation:

```text
MVCC RowSet
  -> read early columns
  -> exact pushed Selection in C++
  -> final RowSet
  -> read deferred columns
  -> output
```

The stage also returns structured timing, row, byte, and column statistics to
TiDB. It is an explicitly enabled preview for pure-columnar snapshots; mixed
memtable/unconverted-L0 support remains the Stage 4 GA blocker.

## Prerequisites

- Stages 1 and 2 are merged.
- The storage reader can materialize an arbitrary column set from a RowSet.
- The current TiDB pushdown tests establish which expressions are retained in
  `TableScan.PushedDownFilterConditions`.

## Affected repositories and modules

### tipb

- `contrib/tipb/proto/executor.proto`
- generated C++ and Go bindings

### cloud-storage-engine

- `components/kvengine/src/read.rs`
- `components/kvengine/src/table/columnar/late_materialization.rs`
- columnar tests

### tiflash-columnar-hub

- `hub-runtime/ffi/src/RaftStoreProxyFFI/ProxyFFI.h`
- `hub-runtime/src/interfaces.rs`
- `hub-runtime/src/columnar_impls.rs`
- `hub-runtime/src/cloud_helper.rs`
- `hub-runtime/src/run.rs`

### TiFlash

- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`
- `dbms/src/Storages/StorageDisaggregatedColumnar.h`
- new
  `dbms/src/Storages/ColumnarLateMaterializationCoordinator.{h,cpp}`
- `dbms/src/Interpreters/Settings.h`
- `dbms/src/Flash/Coprocessor/ColumnarScanContext.h`
- focused tests under `dbms/src/Storages/tests/`

### TiDB

- `pkg/util/execdetails/tiflash_stats.go`
- `pkg/util/execdetails/execdetails_test.go`
- planner/core tests for pushed Selection dependency columns

## Plan and runtime protocol

Add optional messages to `executor.proto`. Field numbers must be chosen after
checking the latest tipb schema; do not reuse or renumber existing fields.

```protobuf
message ColumnarLateMaterializationPlan {
  enum Mode {
    EAGER = 0;
    SELECTION = 1;
    VISIBILITY_FIRST_WITHOUT_FILTER = 2;
  }

  Mode mode = ...;
  repeated int64 early_column_ids = ...;
  repeated int64 deferred_column_ids = ...;
  uint64 max_rowset_bytes = ...;
  uint64 row_batch_rows = ...;
  uint64 row_batch_bytes = ...;
}

message ColumnarStageSummary {
  enum Stage {
    SNAPSHOT_AND_CATALOG = 0;
    VISIBILITY_READ = 1;
    MVCC = 2;
    ROUGH_CHECK = 3;
    EARLY_COLUMN_READ = 4;
    EXACT_SELECTION = 5;
    DEFERRED_COLUMN_READ = 6;
    ROW_SOURCE_REPLAY = 7;
    OUTPUT_ASSEMBLY = 8;
  }

  Stage stage = ...;
  uint64 time_ns = ...;
  uint64 input_rows = ...;
  uint64 output_rows = ...;
  uint64 input_bytes = ...;
  uint64 output_bytes = ...;
  uint64 columns = ...;
  uint64 physical_sources = ...;
}
```

Add a repeated `ColumnarStageSummary` and path/fallback metadata to the existing
`ColumnarScanContext`.

The exact Selection summary is intentionally attached to the TableScan
execution summary in the first implementation. TiDB may remove the physical
Selection executor after moving its expressions into the TableScan, so there
is no reliable Selection executor ID to attach a separate context to. This
does not prevent future operator-specific contexts for TopN or Join.

Old readers ignore the new plan. New readers treat absent mode as `EAGER`. Old
TiDB versions ignore the new repeated runtime fields.

## Column partitioning

TiFlash builds the early dependency closure from
`table_scan.getPushedDownFilters()`:

```text
early columns =
    handle/version columns required by storage
  + all input columns referenced by pushed filter expressions
  + columns required to preserve filter result semantics

deferred columns =
    scan output columns - columns already available early
```

The storage-only handle/version columns are not necessarily exposed in the
C++ early block. A projected column used by both the predicate and output is
read once and retained or gathered through the staged reader; it must not be
read a second time as a deferred column.

Construct the expression action with the current
`DAGExpressionAnalyzer`/`FilterTransformAction` stack so collations, casts,
NULL, timezone, and SQL boolean semantics remain identical to the eager path.

## FFI contract

Append staged entries to the existing interface table. Do not reorder existing
function pointers. Update both `ProxyFFI.h` and the checked-in
`interfaces.rs` mirror in the same commit.

Illustrative ABI:

```c
typedef struct ColumnarStagedReadResult {
    uint64_t rows;
    uint64_t batch_token;
    int64_t physical_table_id;
    uint32_t status;
} ColumnarStagedReadResult;

typedef struct ColumnarStagedOpResult {
    uint64_t accepted_rows;
    uint32_t status;
} ColumnarStagedOpResult;
```

Append functions equivalent to:

```text
fn_supports_staged_read
fn_read_early_block
fn_read_staged_column
fn_apply_early_filter
fn_finish_early_phase
fn_read_deferred_block
fn_take_last_staged_error
```

The exact names follow repository conventions. Requirements:

- every early block receives a non-zero opaque `BatchToken`;
- a token belongs to one reader and is single-use;
- the C++ filter contains exactly `rows` entries;
- `fn_apply_early_filter` copies/consumes the filter synchronously;
- the storage reader intersects the batch filter with its candidate RowSet;
- `finish_early_phase` is legal only after all candidate batches are resolved;
- deferred reads are legal only after `finish_early_phase`;
- an error has a structured status plus retrievable message; do not add another
  ambiguous `u64::MAX` sentinel-only API;
- buffers returned by `fn_read_staged_column` retain the same lifetime contract
  as current column buffers.

FFI layout tests must check struct size/alignment and function-table
initialization on both C++ and Rust sides.

## Rust staged reader state

Extend the Stage 2 reader:

```text
Created
  -> PreparingVisibility
  -> ReadingEarlyColumns
  -> FinalizingSelection
  -> ReadingDeferredColumns
  -> Drained

any state -> Failed
```

For every early batch, retain only:

- its RowSet slice or locators;
- early columns that are also required for final output;
- batch token metadata;
- bounded serialization buffers.

Applying the exact C++ filter clears rejected bits from the candidate RowSet.
`finish_early_phase` builds the final GatherPlan. It must reject outstanding
tokens.

The first implementation sets the late-mode `CloudColumnarReaders` inner
concurrency to sequential (`concurrency = 0` or the equivalent direct reader
path). The current worker/channel interface sends complete blocks one way and
cannot support synchronous filter feedback safely. Region/bucket and TiFlash
pipeline parallelism are unchanged. Re-enabling inner concurrency is a
performance follow-up, not a correctness dependency.

## TiFlash coordinator

Add `ColumnarLateMaterializationCoordinator` as an implementation detail of
`RNColumnarInputStream`.

Responsibilities:

1. decide eager versus staged before output;
2. create the early expression header and filter action;
3. request an early block and deserialize only early columns;
4. execute the exact pushed predicate;
5. return its filter with the `BatchToken`;
6. after all early batches, finalize the storage RowSet;
7. request and deserialize deferred output blocks;
8. merge retained early/output columns into final blocks;
9. collect C++ exact-filter and assembly statistics.

`RNColumnarSourceOp` continues to delegate to `RNColumnarInputStream`, so both
the BlockInputStream and Pipeline engines use the same staged state machine.
Do not introduce a generic `ColumnarMaterializeOp` in this stage.

Avoid reusing the existing name `RNColumnarReaderMaterializeState`; that state
tracks asynchronous reader creation and has different semantics.

## Pushed-filter ownership

Current eager behavior merges residual `filter_conditions` with TableScan
pushed filters and evaluates them after reading a complete block.

In staged mode:

- `table_scan.getPushedDownFilters()` is evaluated exactly once in the
  coordinator's early phase;
- only residual `filter_conditions` stay in the downstream filter stream/action;
- storage rough-check is not exact and never removes the C++ exact evaluation.

In eager mode, preserve the current merged behavior. Add a test where TiDB
removes the physical Selection entirely, proving that the staged TableScan
still evaluates its pushed predicate.

## Settings and path selection

Add to `dbms/src/Interpreters/Settings.h`:

```text
columnar_enable_late_materialization = false
columnar_enable_visibility_first_without_filter = false
columnar_late_materialization_max_rowset_bytes
columnar_late_materialization_row_batch_rows
columnar_late_materialization_row_batch_bytes
```

Capture values in `RNColumnarReaderSharedContext` or the staged read plan at
reader creation. `dt_enable_bitmap_filter` remains required for pushed filter
execution.

Pure-columnar Selection is chosen only when:

- `keep_order = false`;
- the main setting is enabled;
- a pushed Selection exists and its dependency columns are available;
- storage supports every source in the snapshot;
- estimated memory is within the configured limit;
- the FFI capability probe succeeds.

Otherwise record a fallback reason and use eager mode before output.

## Structured statistics

Rust reports storage stages; C++ adds exact Selection and output assembly.
Aggregation rules in `ColumnarScanContext`:

- times, rows, and bytes: sum across readers/tasks;
- physical table count and stage column count: use the field's explicit
  semantic, not the current blanket `max` rule;
- path/fallback counts: sum by enum/reason;
- peaks such as RowSet bytes: max;
- totals such as allocated RowSet bytes: sum.

Required top-level metadata:

- selected path;
- fallback reason;
- RowSet bytes and selected bits;
- early/deferred column counts;
- source counts by columnar/memtable/unconverted-L0;
- physical runs and packs touched.

TiDB:

- parses absent and present stage summaries;
- merges repeated stage entries by stage enum;
- prints concise groups without changing old strings when fields are absent;
- includes units and distinguishes physical bytes from serialized/output bytes.

## Commit and landing plan

Land in dependency order:

1. **tipb: add optional plan and stage-summary messages**
   - Generate C++/Go bindings.
   - Add backward-compatibility serialization tests.
2. **cloud-storage-engine: add tokenized staged Selection API**
   - Implement state machine, early batches, filter intersection, and final
     GatherPlan.
3. **columnar-hub: append staged FFI**
   - Update C header and Rust mirror together.
   - Add ABI layout, token misuse, and error tests.
4. **TiFlash: add settings and coordinator**
   - Keep setting disabled.
   - Share implementation through `RNColumnarInputStream`.
5. **TiFlash: fix filter ownership in staged mode**
   - Evaluate pushed filters early and residual filters downstream.
   - Preserve eager behavior.
6. **TiFlash: serialize structured runtime details**
   - Merge Rust and C++ counters.
7. **TiDB: parse and display stage summaries**
   - Update tipb dependency and execution-detail tests.
8. **Full stack: add pure-columnar Selection tests**
   - Enable only within the test session/configuration.

Each repository commit must build with late materialization disabled. Submodule
bumps are isolated so they can be reviewed or reverted independently.

## Validation

Cloud-storage-engine and hub:

```bash
make format
make clippy
cargo test --package kvengine --lib -- table::columnar::late_materialization --nocapture
cargo test --package kvengine --lib -- tests::test_columnar --nocapture
```

TiFlash:

```bash
cmake --build --preset unit-tests
cmake-build-debug/dbms/gtests_dbms --gtest_filter='*ColumnarLateMaterialization*'
```

Also run the existing next-generation columnar full-stack suite after adding a
dedicated late-materialization case to its runner.

TiDB:

```bash
go test ./pkg/util/execdetails
go test ./pkg/planner/core/...
```

Narrow planner packages after locating the final test files, but retain a test
covering removal of the pushed physical Selection.

Required end-to-end cases:

- predicate column not in final projection;
- predicate column also in final projection;
- multiple predicate columns and computed expressions;
- SQL NULL/three-valued logic;
- all-pass, all-reject, and sparse filters;
- pushed Selection removed from TiDB executor tree;
- multiple Region buckets and physical table IDs;
- both TiFlash execution engines;
- invalid/reused token and filter-length mismatch;
- cancellation in early, exact-filter, and deferred phases;
- rowset-budget and unsupported-source fallback before output;
- old/new tipb runtime-details combinations.

## Exit criteria

- Pure-columnar staged Selection is multiset-equivalent to eager execution.
- A pushed predicate is evaluated exactly once regardless of whether TiDB
  keeps a Selection executor.
- Both execution engines use the same coordinator semantics.
- Runtime details reach TiDB with stage time, rows, bytes, and columns.
- Old components tolerate absent/new optional fields.
- Late mode does not use the current one-way Rust inner worker pipeline.
- The feature remains off by default and row-source snapshots fall back before
  output with an observable reason.

## Rollback

Disable `columnar_enable_late_materialization` for new readers. Existing readers
finish with their captured setting. Protocol additions remain backward
compatible and may stay in place during rollback. No reader changes mode after
it emits output.
