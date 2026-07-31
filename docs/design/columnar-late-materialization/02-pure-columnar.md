# Stage 2: Pure-Columnar Visibility-First Materialization

## Goal

Use the Stage 1 MVCC RowSet to read projected payload columns from columnar
files in physical file/pack order.

This stage proves the storage-side I/O architecture and introduces the
projection-only visibility-first candidate. It does not yet implement the
C++ exact Selection feedback loop; that arrives in Stage 3.

## Prerequisites

- Stage 1 `SourceCatalog`, `RowLocator`, `RowSet`, and global MVCC builder are
  merged.
- Its differential tests pass for L0/L1/L2 pure-columnar snapshots.
- The storage invariant documentation is updated.

## Scope

Included:

- conversion of a RowSet into source/pack physical runs;
- deferred reading of arbitrary projected column IDs;
- output block assembly in physical source order;
- projection-only visibility-first mode;
- pack-clean versus visibility-first path policy;
- post-MVCC predicate rough-check integration point;
- storage-side stage metrics and benchmarks.

Excluded:

- exact predicate evaluation in C++;
- staged filter FFI;
- row-source replay;
- TiDB runtime-details protocol;
- default enablement.

## Affected modules

- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/late_materialization.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/read.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/tests/test_columnar.rs`
- `contrib/cloud-storage-engine/tests/cloud_engine/columnar/`
- `contrib/tiflash-columnar-hub/hub-runtime/src/`
- `contrib/cloud-storage-engine/MAINTAINER_GUIDE.md`

The columnar-hub work in this stage may remain Rust-internal or expose only a
disabled capability probe. The public staged C ABI is delivered atomically in
Stage 3.

## Storage reader

Add a dedicated reader rather than adding mode-dependent behavior throughout
`ColumnarTableReader`:

```rust
enum LateMaterializationMode {
    VisibilityFirstWithoutFilter,
    // Selection is activated in Stage 3.
}

struct ColumnarLateMaterializationReader {
    snap_access: Arc<SnapAccess>,
    source_catalog: Arc<SourceCatalog>,
    mvcc_rowset: RowSet,
    final_rowset: RowSet,
    gather_plan: GatherPlan,
    projected_columns: Vec<ColumnId>,
    state: ReaderState,
    stats: LateMaterializationStats,
}
```

The current eager reader stays intact. `read.rs` performs path selection before
creating either reader.

## GatherPlan and physical runs

Transform each source mask into monotonically increasing physical runs:

```rust
struct PhysicalRun {
    source_id: SourceId,
    first_ordinal: u64,
    row_count: u32,
    selected_mask: RunMask,
}

struct GatherPlan {
    runs: Vec<PhysicalRun>,
    selected_rows: u64,
}
```

Runs are:

- grouped by `SourceId`;
- ordered by file/pack physical position;
- bounded so one run does not require unbounded temporary memory;
- able to represent a partially selected pack without issuing one seek per
  row.

For Selection, final row order is not semantically observable because
`keep_order = false`. Stage 2 therefore emits survivors in `GatherPlan` physical
order. A future TopN plan may add scatter positions but does not change the
RowSet representation.

## Deferred column reader

For each run:

1. open or reuse the immutable columnar source from `SourceDescriptor`;
2. seek to the pack/range containing `first_ordinal`;
3. read the requested projected columns sequentially;
4. apply the run's selected mask while decoding or assembling the output;
5. append selected values to a bounded output block;
6. stop when the output row/byte limit is reached.

The API is column-set based, not row-at-a-time:

```rust
fn materialize_columns(
    &mut self,
    columns: &[ColumnId],
    max_rows: usize,
    max_bytes: usize,
) -> Result<Option<MaterializedBlock>>;
```

The reader must not create one random seek per locator. Tests and metrics count
physical runs, packs touched, and selected rows per touched pack so regressions
are visible.

## Projection-only mode

When no pushed Selection is staged:

```text
final RowSet = MVCC RowSet
```

The deferred reader reads only visible rows of projected payload columns. This
is useful when MVCC amplification is high, but it is not always better than the
existing pack-clean path.

Add the candidate behind:

```text
columnar_enable_visibility_first_without_filter = false
```

The setting is defined in TiFlash in Stage 3; before that, storage tests select
the mode directly.

## Pack-clean path policy

The existing pack-clean optimization remains eligible only under its current
correctness conditions, including:

- pure L2 columnar input;
- no merge requirement;
- projected columns exclude handle/version where required by current logic;
- a complete clean pack;
- pack maximum version visible at the snapshot.

Choose a path once, before output:

```text
if pack-clean is valid and policy prefers it:
    use existing pack-clean reader
else if visibility-first candidate is enabled and supported:
    use ColumnarLateMaterializationReader
else:
    use existing eager MVCC reader
```

The first policy may be conservative:

- prefer pack-clean whenever it is valid;
- use visibility-first for overlapping versions, deletes, or non-clean packs;
- record why each candidate was accepted or rejected.

Do not force visibility-first merely because its feature setting is on. Later
cost tuning must be benchmark-driven.

## Predicate rough-check placement

Stage 2 provides a method to intersect an already correct MVCC RowSet with
pack-level predicate candidates:

```text
candidate RowSet = MVCC RowSet AND rough-check pack mask
```

The rough-check mask may discard entire packs only after global MVCC has picked
the visible physical version. Exact Selection is still required for every
candidate row and is implemented in Stage 3.

The visibility cursor itself must remain independent of the pushed predicate.

## State machine

Projection-only reads use:

```text
Created
  -> PreparingVisibility
  -> PreparingGatherPlan
  -> ReadingDeferredColumns
  -> Drained
```

The following are fatal reader errors:

- a RowSet and catalog have different snapshot identities;
- an ordinal is outside its source range;
- a projected column has a different physical row count from handle/version;
- a state method is called out of order;
- cancellation or a storage read fails.

No eager fallback is allowed after entering `ReadingDeferredColumns`.

## Metrics

Collect storage-local counters now so Stage 3 can expose them without changing
reader semantics:

- catalog source counts by kind/level;
- visibility rows and bytes read;
- MVCC input and visible rows;
- RowSet bytes and set bits;
- rough-check packs input/accepted/rejected;
- gather runs and packs touched;
- deferred rows and bytes read by column;
- output rows and bytes;
- visibility, MVCC, gather-plan, deferred-read, and assembly durations;
- path candidate, selected path, and fallback reason.

Do not overload existing flat fields with a different meaning. Stage 3 maps
these counters into structured protobuf stages.

## Commit plan

1. **Build GatherPlan from source masks**
   - Add run coalescing and pack-boundary unit tests.
2. **Add deferred pure-columnar column materializer**
   - Read selected rows in physical order.
   - Preserve nullable/default/column decode behavior of the eager path.
3. **Add projection-only late reader**
   - Wire MVCC RowSet to GatherPlan and bounded output blocks.
   - Keep production selection disabled.
4. **Integrate post-MVCC rough-check masks**
   - Verify rough-check never changes MVCC winner selection.
5. **Add pack-clean/eager/visibility-first path selector**
   - Preserve all existing pack-clean guards.
   - Add explicit decision reasons.
6. **Add metrics, differential tests, and microbenchmarks**
   - Compare output and I/O counters across all three paths.
7. **Update storage documentation**
   - Document physical-order emission and path-selection invariants.

## Tests

Required correctness cases:

- projection subsets including nullable and variable-length columns;
- zero payload columns, handle-only, and version requested in output;
- int/common handles and multiple physical table IDs;
- partial first/last packs and sparse selections;
- selected runs spanning pack boundaries;
- all-visible, highly versioned, and delete-heavy snapshots;
- rough-check all-pass/all-reject/partial;
- pack-clean valid and invalid cases;
- cancellation between runs;
- injected deferred-column read/decode failure;
- output row/byte block limits.

Required performance comparisons:

- clean L2 projection scan: pack-clean must not regress through forced
  visibility-first selection;
- high MVCC amplification: visibility-first should reduce payload bytes;
- sparse RowSet: physical run count and read amplification are bounded;
- wide projection: visibility-first overhead is recorded even when it loses.

Run from `contrib/cloud-storage-engine`:

```bash
make format
make clippy
cargo test --package kvengine --lib -- table::columnar::late_materialization --nocapture
cargo test --package kvengine --lib -- tests::test_columnar --nocapture
cargo test --package tests --test cloud_engine --features testexport columnar -- --nocapture
```

Run columnar-hub Rust tests if its internal reader integration changes.

## Exit criteria

- Pure-columnar deferred output is multiset-equivalent to the eager reader.
- Payload reads follow source/pack physical order and are not per-row random
  reads.
- Rough-checking happens after MVCC and exact filtering remains mandatory.
- Existing pack-clean behavior and its correctness guards are preserved.
- Metrics distinguish visibility, MVCC, rough-check, and deferred payload work.
- The production C++ query path remains disabled.

## Rollback

The reader is selected only by an explicit internal/experimental mode. Disable
the mode before reader creation to return to the current pack-clean/eager path.
No on-disk format or snapshot metadata changes are introduced.
