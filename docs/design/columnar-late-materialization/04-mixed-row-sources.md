# Stage 4: Memtable and Unconverted-L0 Support

## Goal

Make staged Selection and projection-only visibility-first reads work when the
snapshot contains memtables or unconverted L0 row sources.

This is required for production enablement. Source presence must not force the
query back to eager full-row decoding once this stage is complete.

## Prerequisites

- Stage 1 SourceCatalog includes each row source separately.
- Stage 1 global MVCC accepts a source-aware visibility cursor.
- Stage 2 materialization accepts multiple source kinds behind a common
  GatherPlan.
- Stage 3 staged Selection/FFI is available for pure-columnar snapshots.

Stage 4 can begin after Stage 1 contracts stabilize and integrate with Stage 3
later.

## Current limitation

`collect_column_row_readers` currently builds iterators for memtables and
unconverted L0 files, then collapses them through `RowMvccReader`.
`AsyncMergeIterator` resolves ordering but does not expose a stable physical
source and source-local ordinal to the caller.

`ColumnarRowTableReader` subsequently decodes complete row values and resolves
Blob references while constructing projected columns. Reusing that flow would
pay the payload cost before Selection and defeat delayed materialization.

The implementation therefore separates:

1. source-aware row visibility;
2. immutable replay of selected row locators;
3. projection-aware row-value decode.

## Affected modules

Primary:

- `contrib/cloud-storage-engine/components/kvengine/src/read.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/reader.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/late_materialization.rs`
- row iterator/merge modules used by `build_prefixed_row_merge_iter`
- `contrib/cloud-storage-engine/components/kvengine/src/tests/test_columnar.rs`
- `contrib/cloud-storage-engine/tests/cloud_engine/columnar/`
- `contrib/cloud-storage-engine/MAINTAINER_GUIDE.md`

Integration:

- `contrib/tiflash-columnar-hub/hub-runtime/src/columnar_impls.rs`
- TiFlash runtime-detail aggregation and full-stack tests

No new C++ filter semantics are required; the Stage 3 coordinator remains the
consumer.

## Source-aware row visibility

Replace the late-mode use of the collapsed row reader with individual
source-aware cursors:

```rust
struct RowVisibilityRecord {
    handle: HandleRefOrOwned,
    version: u64,
    is_delete: bool,
    locator: RowLocator,
    stable_source_priority: u32,
}

trait RowVisibilityCursor {
    fn current(&self) -> Result<Option<RowVisibilityRecord<'_>>>;
    fn next(&mut self) -> Result<()>;
}
```

The cursor reads only key/write metadata needed to derive:

- table handle;
- commit/version order;
- delete state;
- source-local physical ordinal.

It must not fetch a default/write value blob or decode row v1/v2 merely to
decide visibility.

Merge row cursors together with columnar visibility cursors using Stage 1's
single global ordering:

```text
(handle ascending, version descending, stable source priority ascending)
```

This is one MVCC pass across all source kinds. Running independent row and
columnar MVCC passes and unioning their results is incorrect when versions of
one handle are split across source kinds.

## Row-source ordinal

The source-local ordinal is the number of physical iterator entries visited
within that source's scan range, including entries that lose MVCC. Its
definition must be reproducible by a fresh iterator created from the same
`SnapAccess`, `SourceDescriptor`, and key range.

For an iterator that may skip internal entries while seeking, the descriptor
must store enough immutable positioning information to replay the same ordinal
sequence. If ordinal replay cannot be proven stable for a source type, use a
source-local immutable key/version locator internally while preserving the
public `(SourceId, ordinal)` contract through a source index. Do not keep
borrowed iterator/value pointers in the RowSet.

Required debug/test assertion on replay:

```text
resolved handle/version/delete == visibility record for locator
```

A mismatch fails the reader; it never produces a different row.

## Immutable source replay

Add a `RowSourceMaterializer` that creates fresh readers from the immutable
snapshot:

```rust
trait SourceMaterializer {
    fn read_early(
        &mut self,
        locators: &[RowLocator],
        columns: &[ColumnId],
        limits: BatchLimits,
    ) -> Result<MaterializedBatch>;

    fn read_deferred(
        &mut self,
        locators: &[RowLocator],
        columns: &[ColumnId],
        limits: BatchLimits,
    ) -> Result<MaterializedBatch>;
}
```

Implementation rules:

- group locators by source and increasing ordinal;
- scan/reseek monotonically within a source;
- stop at the configured row or owned-byte batch limit;
- reuse existing decryption, schema/default, row-v1/v2, and Blob resolution
  helpers rather than duplicating their semantics;
- keep physical-table identity with every materialized batch;
- release replay iterator state on cancellation or failure.

The RowSet stores locators only. It does not retain complete encoded row values
for the scan lifetime.

## DeferredRowValue

For the current bounded batch only:

```rust
enum DeferredRowValue {
    Inline {
        encoded: Bytes,
    },
    Blob {
        reference: BlobRef,
        fetched: Option<Bytes>,
    },
}
```

The concrete representation may avoid an extra copy, but it must be owned and
must not borrow iterator memory across an FFI call.

Behavior:

- handle/version-only visibility does not construct `DeferredRowValue`;
- if early predicate columns require the row value, fetch/decode only those
  columns for candidate locators;
- discard rejected candidates and their owned bytes immediately after filter
  feedback;
- decode deferred projected columns only for survivors;
- if an early column is also projected, retain its decoded value for the
  current batch or gather it without refetching;
- a BlobRef is fetched only when a semantically required early or deferred
  column needs it.

## Error equivalence

Delayed reads may avoid an error in an invisible or rejected payload that eager
execution happened to touch. The correctness contract is:

- snapshot/MVCC errors are always observed;
- errors in values required to evaluate the predicate are observed;
- errors in projected values of surviving rows are observed;
- errors in values of invisible or exactly rejected rows are not required to
  be observed.

Tests must distinguish valid lazy avoidance from a missed required error.

Schema defaults, casts, malformed row encoding, encryption, and Blob errors for
a surviving row must match the eager path.

## Mixed-source GatherPlan

After exact Selection, build one GatherPlan containing:

- columnar physical runs;
- memtable locator runs;
- unconverted-L0 locator runs.

Each source is read in its most sequential available order. With
`keep_order = false`, completed source batches may be emitted without sorting
back by handle/version. MVCC correctness is already encoded by the final
RowSet.

Do not wait for all row sources to decode before beginning columnar deferred
reads. Use a bounded output queue and fair source scheduling so one slow Blob
source does not cause unbounded completed-block retention.

## Memory limits

Account explicitly for:

```text
RowSet masks and locators
+ source replay cursors
+ inline encoded values in current batch
+ BlobRef metadata and fetched Blob bytes
+ decoded early columns
+ retained early/output columns
+ completed output blocks in the bounded queue
```

Enforce both:

- `columnar_late_materialization_row_batch_rows`;
- `columnar_late_materialization_row_batch_bytes`.

An individual row larger than the byte limit is allowed as a one-row batch,
subject to the query memory limit. This avoids infinite retry/splitting loops.

Fallback for an estimated RowSet budget overflow occurs before output. Runtime
batch memory overflow is a normal memory-limit error, not a mid-stream eager
fallback.

## Statistics

Extend Stage 3 summaries with:

- row sources by kind;
- row visibility entries/bytes;
- selected row locators by kind;
- replay seeks and entries skipped;
- inline values retained and bytes;
- BlobRefs encountered;
- Blob fetch count/bytes/time split by early and deferred phase;
- row decode rows/bytes/time split by early and deferred phase;
- defaults/schema conversions;
- peak owned row-value bytes;
- output queue peak rows/bytes.

The `ROW_SOURCE_REPLAY` stage is present only when used. Old TiDB versions
ignore it.

## Commit plan

1. **Expose individual row sources in SourceCatalog**
   - Preserve current stable precedence.
   - Add memtable and unconverted-L0 descriptors.
2. **Add source-aware row visibility cursors**
   - Produce locator/handle/version/delete without row decode.
   - Merge globally with columnar cursors.
3. **Add immutable locator replay**
   - Reopen a source from `SnapAccess`.
   - Assert handle/version identity and monotonic replay.
4. **Extract/reuse projection-aware row decode**
   - Share semantics with `ColumnarRowTableReader`.
   - Avoid duplicating defaults, schema, and row-version logic.
5. **Add DeferredRowValue and BlobRef staging**
   - Separate early and deferred fetch/decode counters.
6. **Integrate mixed sources with staged FFI**
   - Reuse Stage 3 token and filter protocol.
   - Add bounded fair output scheduling.
7. **Add memory/cancellation/fault tests and statistics**
   - Verify release at every state transition.
8. **Remove row-source preview fallback**
   - Stop accepting memtable/unconverted-L0 as a normal fallback reason.
   - Update `MAINTAINER_GUIDE.md`.

Keep commits 1–7 behind the disabled feature until the differential matrix is
green. Commit 8 is the explicit behavior transition and should be easy to
revert.

## Validation matrix

Storage combinations:

- memtable only;
- unconverted L0 only;
- converted columnar only;
- memtable + L0;
- memtable + L0 + L1/L2;
- multiple row sources with equal handles;
- a newer row version over an older columnar version;
- a newer columnar delete over an older row value;
- equal handle/version across source kinds.

Value combinations:

- inline row v1 and row v2;
- BlobRef;
- common and int handles;
- NULL, missing column/default, added/dropped column;
- early-only, deferred-only, and early-plus-projected columns;
- malformed/rejected versus malformed/surviving values;
- encrypted source read failure.

Resource/fault combinations:

- all-pass/all-reject/sparse filters;
- a value larger than batch byte limit;
- cancellation before Blob fetch, during fetch, after filter feedback, and
  during deferred decode;
- RowSet estimate fallback;
- query memory limit during an oversized row;
- source replay identity mismatch injection;
- multiple Region buckets.

Run the Stage 1–3 storage, TiFlash, TiDB, and full-stack suites plus the new
mixed-source tests. Add failpoints or syncpoints where deterministic
cancellation/error timing is needed.

## Exit criteria

- Memtable and unconverted-L0 presence no longer causes normal fallback.
- One global MVCC pass resolves versions across all source kinds.
- Visibility does not decode or fetch row payload.
- Inline and BlobRef survivors are multiset-equivalent to eager output.
- Rejected row values are released after bounded batches.
- Required payload errors match eager behavior.
- Peak owned row-value memory is reported and bounded.
- Source replay mismatch fails safely.

## Rollback

Before GA, a dedicated mixed-source enable policy may be disabled so affected
snapshots choose eager mode before output. After the implementation is proven,
the general kill switch remains the supported rollback. Reverting the final
behavior-transition commit restores the preview fallback without reverting
the source-aware storage work.
