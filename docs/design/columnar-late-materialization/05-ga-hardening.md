# Stage 5: Production Hardening and GA Rollout

## Goal

Turn the completed pure-columnar and mixed-source implementation into a
production feature with verified compatibility, bounded resources, useful
runtime details, conservative path selection, and a tested online rollback.

This stage does not add new MVCC or operator semantics.

## Prerequisites

- Staged Selection is correct for pure-columnar snapshots.
- Memtable and unconverted-L0 snapshots use the same optimization.
- Both TiFlash execution engines share the RN staged coordinator.
- Structured stage summaries reach TiDB.
- The eager differential matrix is green.

## Scope

Included:

- cross-version and upgrade/downgrade compatibility;
- fault injection and long-running stress;
- memory, cancellation, and lifecycle audits;
- conservative path policy and performance gates;
- metrics, logs, dashboards, and alerts;
- canary rollout, defaults, and rollback drills;
- removal of preview-only row-source fallback.

Excluded:

- TopN or Join;
- ordered scans;
- a generic materialization operator;
- mandatory Rust inner physical-table concurrency;
- on-disk format changes.

## Compatibility matrix

Test at least:

| TiDB | TiFlash/tipb | Expected behavior |
| --- | --- | --- |
| Old | Old | Existing eager behavior |
| Old | New | New TiFlash defaults to eager; unknown runtime fields ignored |
| New | Old | Capability absent; TiDB/TiFlash use eager behavior |
| New | New, setting off | Existing eager behavior with no semantic change |
| New | New, setting on | Staged path when eligible |

Also test rolling:

- TiFlash nodes upgraded one at a time;
- TiDB upgraded before and after TiFlash;
- query retries landing on a node with different capability;
- downgrade while optional protobuf fields remain in captured diagnostics.

The staged FFI is local to a TiFlash binary, but its interface table still
needs version/capability validation so a partial submodule mismatch fails at
startup or chooses eager mode, not at the first query.

## Correctness hardening

Expand differential testing with randomized generation:

- arbitrary source counts and level mixtures;
- random handle/version/delete distributions;
- random TSO and key ranges;
- random projected and predicate column subsets;
- NULL/default/schema-change combinations;
- varying pack and output block boundaries;
- inline and BlobRef row values.

Compare:

- result multisets;
- physical table IDs;
- required errors;
- cancellation behavior;
- no duplicated or missing visible handles;
- stage row-conservation invariants.

Useful invariants:

```text
mvcc_output_rows <= visibility_input_rows
rough_check_output_rows <= mvcc_output_rows
exact_selection_output_rows <= early_input_rows
deferred_output_rows == final_rowset_bits
output_rows == sum(deferred output blocks)
```

For multiple buckets, compare the union of bucket outputs with the non-bucketed
eager scan for the same snapshot.

## Fault injection

Add deterministic failpoints/syncpoints at:

- after snapshot/catalog construction;
- during each source's visibility read;
- after MVCC RowSet allocation;
- before and after an early FFI batch;
- while C++ evaluates the filter;
- before filter feedback and after token consumption;
- while building GatherPlan;
- during columnar deferred read;
- during row-source replay and Blob fetch;
- during output serialization/deserialization;
- on cancellation in every state;
- on runtime-detail serialization.

For each point verify:

- outstanding tokens are invalidated;
- RowSet and buffers are released;
- no block is emitted after a fatal error;
- no automatic mid-stream eager fallback occurs;
- retry creates a new snapshot/catalog/reader;
- errors are neither swallowed nor double-reported.

Run long-lived scans under compaction, flush/conversion, Region split, and
snapshot retry activity. Snapshot ownership must isolate the active catalog
from later storage changes.

## Resource validation

Measure and enforce:

- RowSet estimate versus actual bytes;
- peak dense bitmap bytes per query and per bucket;
- candidate/final RowSet coexistence;
- early block and retained-column memory;
- row replay/Blob batch memory;
- completed-output queue bytes;
- reader and source handle count;
- cancellation release latency.

Add guards for multiplication overflow when estimating:

```text
source rows * masks
batch rows * variable-width estimate
reader count * per-reader budget
```

The query memory tracker remains the hard limit. Configuration limits control
path selection and batching; they do not exempt allocations from tracking.

## Path-selection policy

Start with deterministic rules and collect counterfactual metrics where cheap:

1. feature/capability and `keep_order = false`;
2. memory estimate within limit;
3. supported schema/source kinds;
4. preserve valid pack-clean projection scans;
5. for Selection, require enough deferred payload or expected filtering benefit
   to cover the visibility/feedback cost;
6. otherwise use eager mode before output.

Inputs may include:

- early versus deferred column count and estimated widths;
- pushed-filter selectivity estimate from TiDB;
- number/level/kind of physical sources;
- version/delete amplification estimate;
- pack-clean eligibility;
- remote versus cached bytes;
- RowSet estimate.

Do not make the first GA depend on a complex learned cost model. Record chosen
path and rejection reason so thresholds can be tuned from production evidence.

The projection-only mode keeps its independent setting until benchmarks show a
safe default policy.

## Performance gates

Benchmark representative workloads:

- highly selective predicate on a narrow early column and wide projection;
- low-selectivity predicate;
- predicate column equal to projected column;
- clean L2 projection-only scan;
- high version amplification;
- delete-heavy scans;
- many small files versus few large files;
- pure-columnar versus memtable/L0-heavy snapshots;
- inline row values versus BlobRef;
- local cache hit/miss and remote reads;
- one bucket versus many buckets.

Track:

- wall time and time to first output;
- CPU per stage;
- physical and remote bytes by early/deferred phase;
- serialization/deserialization bytes;
- RowSet and peak query memory;
- physical runs, packs touched, and replay seeks;
- Blob fetch count/bytes;
- output rows;
- eager fallback rate/reasons.

GA performance gates:

- no material regression for clean-pack workloads because visibility-first was
  forced;
- selective/wide workloads show payload-byte reduction and an agreed latency
  improvement;
- low-benefit workloads choose eager or remain within an agreed regression
  budget;
- mixed-source batching remains bounded;
- disabling the feature returns baseline behavior.

Exact numeric thresholds are selected from benchmark results and recorded in
the rollout issue before changing defaults.

## Observability

### Runtime details returned to TiDB

Verify stage summaries are:

- correctly aggregated across Regions, buckets, and physical tables;
- stable when a stage is absent;
- labelled with clear byte semantics;
- backward compatible in TiDB string and JSON formatting;
- bounded in serialized size.

### TiFlash metrics and logs

Add low-cardinality metrics for:

- selected path;
- fallback reason;
- active staged readers;
- RowSet bytes and budget rejects;
- early/deferred physical bytes;
- row-source and Blob replay;
- invalid protocol/state errors;
- stage latency histograms;
- cancellation and failure counts.

Log the effective controls at reader creation only at an appropriate sampled or
debug level. Do not log per-batch tokens or high-cardinality source IDs in
normal production logs.

### Dashboards and alerts

Dashboards should answer:

- Is the feature actually selected?
- Which stage dominates latency?
- Are payload bytes reduced?
- Are memtable/L0 snapshots still optimized?
- Is RowSet/row-value memory bounded?
- Are Blob reads moving from rejected to surviving rows?
- Which fallback reason blocks adoption?

Alerts cover sustained protocol errors, unexpected mixed-source fallback,
memory-limit increases, and significant latency regression.

## Rollout sequence

1. **Dark validation**
   - Code deployed, settings off.
   - Verify capability and old/new runtime-detail compatibility.
2. **Pure-columnar opt-in**
   - Enable for selected tests/canary workloads.
   - Mixed sources may still be monitored until Stage 4 is fully qualified.
3. **Mixed-source canary**
   - Require zero normal `HAS_ROW_SOURCE` fallback.
   - Compare memory/Blob metrics with eager.
4. **Small production percentage**
   - Rule-based eligible queries only.
   - Daily correctness/performance review.
5. **Broader Selection rollout**
   - Keep projection-only mode independently controlled.
6. **Default-on candidate**
   - Only after all GA gates and rollback drill pass.
7. **GA**
   - Remove preview-only accepted fallback policy.
   - Retain kill switch and structured diagnostics.

Settings are captured per reader. A rollback changes the setting for new
readers, drains existing queries, and does not mutate their active state.

## Commit plan

1. **Add compatibility and randomized differential suites**
2. **Add failpoints, cancellation tests, and lifecycle assertions**
3. **Complete memory accounting and overflow guards**
4. **Add conservative path policy and fallback enums**
5. **Complete TiDB/TiFlash structured observability**
6. **Add benchmark suite and record baseline results**
7. **Add rollout configuration and operational documentation**
8. **Remove preview-only mixed-source fallback**
9. **Change defaults only after the rollout gate is approved**

Keep the default-change commit separate from correctness and protocol changes
so rollback is immediate and reviewable.

## Validation

Run:

- cloud-storage-engine unit, columnar integration, clippy, and format checks;
- columnar-hub ABI and state-machine tests;
- TiFlash unit tests under `ENABLE_NEXT_GEN_COLUMNAR`;
- next-generation columnar full-stack tests in both execution engines;
- TiDB planner and execution-detail tests;
- rolling old/new component matrix;
- ASAN/TSAN suites for new concurrent/lifecycle code;
- long-running mixed-source fault/stress suites;
- documented performance benchmarks.

Before GA, perform an operational drill:

1. enable the feature for canary traffic;
2. confirm TiDB displays stage summaries;
3. trigger an agreed safe fault/fallback scenario;
4. disable the setting;
5. confirm new readers use eager mode and existing readers drain;
6. confirm results and error rates remain correct.

## GA exit criteria

1. No known correctness difference from eager execution.
2. Both TiFlash execution engines use one staged semantic implementation.
3. Memtable, unconverted-L0, and columnar mixed snapshots remain optimized.
4. RowSet, row-value, Blob, and output-queue memory are bounded and tracked.
5. Runtime details reach TiDB and pass old/new compatibility tests.
6. Clean-pack scans are not forced onto a slower path.
7. Fault and cancellation tests show no leaked token, snapshot, or buffer.
8. The online kill switch and drain behavior have been exercised in a rollout
   drill.
9. The default-change commit is independently revertible.

## Post-GA follow-ups

These are separate proposals:

- TopN with scatter positions and limit-aware candidate retention;
- Join with paired nullable locators and multiplicity;
- ordered-read semantics;
- adaptive dense/sparse RowSets;
- bidirectional Rust inner-table parallelism if benchmarks justify it;
- a general materialization operator only if multiple operators demonstrate a
  common contract.
