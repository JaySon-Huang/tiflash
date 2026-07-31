# Stage 1: Source Identity and MVCC RowSet Foundation

## Goal

Introduce the storage-level identity and visibility contracts required by every
future delayed-materialization operator, without changing query output or
enabling a new read path.

At the end of this stage, cloud-storage-engine can build a snapshot-correct
MVCC RowSet for a pure-columnar scan and prove it against the eager reader.
Payload columns are still read by the existing path.

## Scope

Included:

- deterministic physical-source enumeration;
- stable physical row locators;
- handle/version-only cursors for columnar files;
- global MVCC over locators;
- source-local dense RowSets;
- memory estimation and fallback reason types;
- differential test infrastructure.

Excluded:

- deferred payload reading;
- exact Selection feedback;
- C FFI and TiFlash changes;
- row-value replay for memtables or unconverted L0 files;
- runtime protocol changes.

## Affected modules

Primary implementation:

- `contrib/cloud-storage-engine/components/kvengine/src/table/columnar/`
- `contrib/cloud-storage-engine/components/kvengine/src/read.rs`
- `contrib/cloud-storage-engine/components/kvengine/src/tests/test_columnar.rs`
- `contrib/cloud-storage-engine/tests/cloud_engine/columnar/`
- `contrib/cloud-storage-engine/MAINTAINER_GUIDE.md`

Add an internal module:

```text
components/kvengine/src/table/columnar/late_materialization.rs
```

The initial module is storage-internal. Do not expose a C ABI in this stage.

## Data contracts

The following names are illustrative but should remain conceptually stable.

```rust
pub(crate) struct SourceId(u32);

pub(crate) enum SourceKind {
    ColumnarFile,
    MemTable,
    UnconvertedL0,
}

pub(crate) struct SourceDescriptor {
    id: SourceId,
    kind: SourceKind,
    stable_priority: u32,
    physical_table_id: i64,
    estimated_physical_rows: u64,
    // Storage-specific immutable identity required to reopen the source.
}

pub(crate) struct SourceCatalog {
    snapshot_identity: SnapshotIdentity,
    sources: Vec<SourceDescriptor>,
}

pub(crate) struct RowLocator {
    source_id: SourceId,
    source_row_ordinal: u64,
}

pub(crate) struct SourceMask {
    source_id: SourceId,
    base_ordinal: u64,
    row_count: u64,
    words: Vec<u64>,
}

pub(crate) struct RowSet {
    snapshot_identity: SnapshotIdentity,
    scan_range: KeyRange,
    masks: Vec<SourceMask>,
    selected_rows: u64,
}
```

`SnapshotIdentity` does not need to be globally serializable. It must be
sufficient to assert that a catalog, cursors, and RowSet belong to the same
`SnapAccess` and scan range.

### SourceId assignment

Build `SourceCatalog` from the immutable `ShardData` held by `SnapAccess`.
Assign IDs in a deterministic catalog order:

1. source kind according to the same precedence used to resolve identical
   physical versions today;
2. level and existing file ordering for columnar sources;
3. immutable source identity as the final tie-breaker.

The implementation must document the exact mapping after checking the current
source collection order. It must never use an address, channel arrival order,
or heap insertion order.

Do not treat a merged `RowMvccReader` as one physical source. Stage 1 records
each row source in the catalog even though row-source locator production is
implemented in Stage 4.

### Source-local ordinal

For a columnar file, the ordinal is the zero-based physical row position across
the file's packs in storage order. It includes:

- committed and invisible versions;
- deletes;
- rows outside the final MVCC result but inside the source cursor's scan
  interval.

The same row observed by the visibility cursor and the deferred column reader
must have the same ordinal. Tests must cover pack boundaries and non-zero key
range starts.

### RowSet representation

Start with a dense mask per source. `base_ordinal` allows a scan range to avoid
allocating bits for a file prefix that cannot be visited. Bit `i` denotes
physical ordinal `base_ordinal + i`.

The representation is deliberately not a Roaring bitmap in the first version.
The API must hide its storage so a later dense/sparse policy does not affect
MVCC or materialization callers.

## Visibility cursor

Add a pure-columnar visibility cursor that reads only:

- handle or common-handle columns;
- nullable version;
- the delete/tombstone state required by current MVCC rules;
- `RowLocator`.

It must not reuse `ColumnarTableReader` as-is because that reader:

- owns and reads all projected columns in lockstep;
- applies the pushed predicate's rough-check filter while constructing
  handle/version readers.

Applying the Selection rough-check before global MVCC is incorrect: an older
version in a pack accepted by rough-check cannot replace a newer visible
version in a pack rejected by rough-check. Predicate rough-checking is applied
only after the MVCC RowSet exists.

The cursor yields a lightweight record:

```rust
struct VisibilityRecord {
    handle: HandleRefOrOwned,
    version: u64,
    is_delete: bool,
    locator: RowLocator,
    stable_source_priority: u32,
}
```

Handle ownership may be borrowed within a merge step, but it must remain valid
until all records for the current handle are resolved.

## Global MVCC builder

Merge all visibility cursors by:

```text
handle ascending
version descending
stable source priority ascending
```

For each handle, apply exactly the current snapshot TSO, delete, and MVCC
filter rules. Set one bit for the winning visible physical version, or no bit
when the winning state is deleted/invisible.

The merge must explicitly handle an equal `(handle, version)` from more than
one source. The current asynchronous merge reader has a debug assumption that
equal versions do not conflict and is therefore not the implementation oracle
for the new tie case. The chosen stable source priority must match the current
storage precedence, and a differential test must lock down that behavior.

## Memory and cancellation

Before allocating masks:

```text
estimated bytes =
    catalog bytes
  + sum(ceil(source range rows / 8))
  + cursor heap and handle buffers
```

Return a typed decision such as:

```rust
enum LateMaterializationUnavailable {
    Disabled,
    UnsupportedSource,
    RowSetEstimateExceeded { estimated: u64, limit: u64 },
    UnsupportedSchema,
}
```

This is a path-selection result, not a storage error. Once RowSet construction
starts, cancellation and storage errors propagate normally and release all
partial masks.

## Differential test harness

Add a reusable helper that runs:

1. the current eager reader for a fixed snapshot and key range;
2. the new MVCC RowSet builder;
3. a test-only payload gather from all set locators;
4. comparison as an unordered multiset.

Required cases:

- int and common handles;
- a single file and multiple L0/L1/L2 files;
- versions for one handle split across files;
- equal handle/version conflict using stable source priority;
- visible delete and invisible delete;
- TSO before, at, and after versions;
- scan range beginning and ending inside a pack;
- empty snapshot and empty Region bucket;
- pack boundary ordinals;
- cancellation during visibility merge;
- RowSet budget accepted and rejected.

For mixed row/columnar snapshots, this stage verifies catalog enumeration and
returns `UnsupportedSource` before output. End-to-end support belongs to
Stage 4.

## Commit plan

Each commit should compile and pass its focused tests.

1. **Add physical identity value types and unit tests**
   - Add `SourceId`, `RowLocator`, `SourceMask`, `RowSet`, bit operations, and
     memory accounting.
   - No production caller.
2. **Build deterministic SourceCatalog**
   - Enumerate all columnar and row sources from `SnapAccess`.
   - Add stable-priority and snapshot/range ownership assertions.
3. **Add pure-columnar visibility cursors**
   - Read handle/version/delete state without projected payload.
   - Verify ordinals over packs and subranges.
4. **Build global MVCC RowSet**
   - Add deterministic merge and current MVCC rules.
   - Add equal-version and delete tests.
5. **Add differential harness and fallback decisions**
   - Compare gathered RowSet results with eager output.
   - Add memory and cancellation tests.
6. **Document storage invariants**
   - Update `MAINTAINER_GUIDE.md` with SourceId stability, ordinal meaning,
     MVCC ordering, and the prohibition on predicate rough-checking before
     MVCC.

## Validation

Run from `contrib/cloud-storage-engine`:

```bash
make format
make clippy
cargo test --package kvengine --lib -- table::columnar::late_materialization --nocapture
cargo test --package kvengine --lib -- tests::test_columnar --nocapture
cargo test --package tests --test cloud_engine --features testexport columnar -- --nocapture
```

The exact integration-test filter may be narrowed after the new module name is
known, but the existing columnar suite must run before landing.

## Exit criteria

- Source IDs and source-local ordinals are deterministic across repeated
  reader construction for the same snapshot.
- Global MVCC RowSets match eager output for every required case.
- Predicate rough-checking is absent from the visibility pass.
- RowSet budget rejection occurs before output and leaves the eager path
  unchanged.
- Memtable/unconverted-L0 presence is represented explicitly and returns a
  typed preview limitation, not a silent incorrect RowSet.
- No feature setting enables a new production query path yet.

## Rollback

This stage is unreachable from production reads. Reverting its commits removes
only internal types, tests, and documentation; the eager reader and FFI remain
unchanged.
