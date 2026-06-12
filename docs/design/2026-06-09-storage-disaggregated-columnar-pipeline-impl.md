# StorageDisaggregated Columnar Pipeline — Implementation Plan

**Date**: 2026-06-12
**Scope**: Based on `docs/design/2026-06-09-storage-disaggregated-columnar-pipeline.md` and the current codebase, this document defines an implementation plan for integrating `StorageDisaggregated::readThroughColumnar` into the pipeline execution model with correct wait semantics, profile recording, and cancel support.

## 1. Goal And Conclusion

This document is based on the design doc `2026-06-09-storage-disaggregated-columnar-pipeline.md` and a full reading of all related production code. It defines the implementation approach, module boundaries, interface changes, commit split, and validation strategy.

**Conclusion first:**

- **Feasible**: The pipeline model infrastructure (`PipeConditionVariable`, `NotifyFuture`, `OperatorStatus::WAIT_FOR_NOTIFY`, `NullSourceOp`) already exists. The `RNColumnarSourceOp` already extends `SourceOp`. Only application-level changes are needed.
- **Main conflict**: The current `getOrCreateReader()` uses `std::condition_variable::wait()` to block IO threads. The fix is to add a non-blocking `tryGetReadyReader()` for the pipeline path and replace `std::condition_variable` with `PipeConditionVariable`.
- **Key adjustment**: The design doc suggests `RNColumnarReaderNotifyFuture` extending `NotifyFuture`. In the current code, `NotifyFuture` is a pure interface. The cleanest approach is to wrap a `PipeConditionVariable` in a `NotifyFuture` adapter rather than modifying `PipeConditionVariable` itself (which is used broadly).
- **Recommended shape**: 5 commits following the design doc's phases, each independently reviewable and revertible.

## 2. Design Doc And Code Verification

### Documents read
- `docs/design/2026-06-09-storage-disaggregated-columnar-pipeline.md` — full read
- `docs/design/2023-06-07-tiflash-pipeline-model.md` — sections 1-100 (overview + task scheduler)

### Production code read (all full reads unless noted)
| File | What was verified |
|---|---|
| `Storages/StorageDisaggregatedColumnar.h` | `RNColumnarSourceOp`, `RNColumnarReadTask`, `RNColumnarReaderWork`, `RNColumnarInputStream`, `RNColumnarReaderPlan`, state enums |
| `Storages/StorageDisaggregatedColumnar.cpp` | Full implementation: `readThroughColumnar` (pipeline overload), `executeIOImpl`/`awaitImpl`/`readImpl` of `RNColumnarSourceOp`, `getOrCreateReader` (blocking), `prefetchReaderWork` (detached thread), `tryAcquireReaderWork`, `RNColumnarInputStream::readImpl`, `createColumnarReader` FFI, `buildColumnarReadTaskWithBackoff`, region error replan logic |
| `Operators/ExchangeReceiverSourceOp.h` | Reference SourceOp with `WAIT_FOR_NOTIFY` |
| `Operators/ExchangeReceiverSourceOp.cpp` | Pattern: `tryReceive` → `WAIT_FOR_NOTIFY` on empty, decode + fill block_queue on data |
| `Flash/Planner/Plans/PhysicalExchangeReceiver.cpp` | Pipeline source op construction + `addInboundIOProfileInfos` |
| `Flash/Planner/Plans/PhysicalTableScan.cpp` | `buildPipeline` dispatching to `StorageDisaggregatedInterpreter`, then `buildProjection` |
| `Flash/Pipeline/Exec/PipelineExec.cpp` | Execution loop: `execute()` → `fetchBlock()` → `executeIO()` → `await()`, status transitions |
| `Flash/Pipeline/Exec/PipelineExecBuilder.cpp` | `getCurIOProfileInfos()`, `getCurProfileInfos()`, `getCurrentHeader()`, `addConcurrency` logic |
| `Operators/Operator.h` | `SourceOp`, `OperatorStatus` enum |
| `Flash/Pipeline/Schedule/Tasks/NotifyFuture.h` | `NotifyFuture` interface, `current_notify_future` thread-local, `setNotifyFuture()` / `registerTaskToFuture()` |
| `Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h` | `registerTask()`, `notifyAll()`, `notifyOne()`, `notifyTaskDirectly()`, metrics |
| `Flash/Pipeline/Schedule/Tasks/Task.h` (lines 48-80) | `NotifyType` enum (includes `WAIT_ON_TABLE_SCAN_READ`) |
| `Operators/NullSourceOp.h` | Empty-block emitter for empty reader scenarios |
| `Flash/Coprocessor/StorageDisaggregatedInterpreter.h` | Dispatch: `storage->read(exec_context, group_builder, ...)` → `readThroughColumnar` |
| `Storages/StorageDisaggregated.h` | Pipeline overload signatures |
| `Storages/StorageDisaggregated.cpp` (lines 58-101) | `read()` dispatch: `isReadColumnar()` → `readThroughColumnar` |
| `Storages/StorageDisaggregatedRemote.cpp` (lines 844-881) | `buildRemoteSegmentSourceOps`: profile recording pattern for tiflash-write path |
| `DataStreams/AddExtraTableIDColumnTransformAction.h` | `buildHeader()` used by `RNColumnarSourceOp::setHeader()` |

## 3. Current-State Findings

### 3.1 Pipeline model infrastructure already exists

The pipeline scheduler supports the full state machine described in the design doc:

```
OperatorStatus → TaskStatus mapping:
  HAS_OUTPUT / NEED_INPUT → CPU task thread pool
  IO_IN / IO_OUT          → IO task thread pool
  WAITING                 → Wait Reactor (polling)
  WAIT_FOR_NOTIFY         → registered to NotifyFuture
```

`PipelineExec::execute()` calls `fetchBlock()` which walks SourceOp → read(), then transform ops, then sink op. At each step, `IO_IN/IO_OUT` stores the operator as `io_op`, `WAIT_FOR_NOTIFY` stores as `waiting_for_notify`. The scheduler then routes the task accordingly.

### 3.2 ExchangeReceiver is the gold-standard pattern

`ExchangeReceiverSourceOp::readImpl()`:
1. `exchange_receiver->tryReceive(stream_id, recv_msg)` — non-blocking, sets `current_notify_future` internally
2. On empty: returns `WAIT_FOR_NOTIFY`
3. On data: decodes packet → fills `block_queue` → pops one block → returns `HAS_OUTPUT`
4. On EOF: returns `HAS_OUTPUT` with empty block

No `awaitImpl()` override is needed — the operator returns `WAIT_FOR_NOTIFY` directly from `readImpl()`. The scheduler calls `registerTaskToFuture()` which reads thread-local `current_notify_future`.

### 3.3 Current RNColumnarSourceOp has critical gaps

**Gap 1: IO thread blocking** (`StorageDisaggregatedColumnar.cpp:1565-1600`)
- `executeIOImpl()` creates `RNColumnarInputStream` → calls `current_input_stream->read()` → `RNColumnarInputStream::readImpl()` → `ensureReader()` → `task->getOrCreateReader()` → `std::condition_variable::wait()` when state is `Creating`
- This blocks the IO thread potentially for seconds (region miss, lock resolve, PD backoff)

**Gap 2: No WAIT_FOR_NOTIFY path** (`StorageDisaggregatedColumnar.cpp:1555-1563`)
- `awaitImpl()` unconditionally returns `IO_IN` — there is no code path that returns `WAIT_FOR_NOTIFY`
- The operator never calls `setNotifyFuture()` or sets up a `NotifyFuture`

**Gap 3: Missing profile recording** (`StorageDisaggregatedColumnar.cpp:666-714`)
- `readThroughColumnar()` (pipeline overload) creates `RNColumnarSourceOp` but never calls `addInboundIOProfileInfos` / `addOperatorProfileInfos`
- Compare with `buildRemoteSegmentSourceOps` (tiflash-write path, `StorageDisaggregatedRemote.cpp:875-880`) which records both profiles immediately after source ops

**Gap 4: No empty reader handling** (`StorageDisaggregatedColumnar.cpp:666-714`)
- If `read_columnar_tasks.empty()` or `getReaderCount() == 0`, the code skips creating any source ops but still proceeds to `executeGeneratedColumnPlaceholder`, `extraCast`, `filterConditionsWithPushedDownFilters`
- `group_builder.getCurrentHeader()` will fail because the builder is empty
- Compare with `DAGStorageInterpreter` which uses `NullSourceOp`

**Gap 5: No cancel semantics**
- `RNColumnarReadTask` has no `cancel()` method
- `RNColumnarSourceOp::operateSuffixImpl()` doesn't cancel unfinished task
- Prefetch thread has no cancel check

**Gap 6: Prefetch thread bypasses scheduler**
- `prefetchReaderWork()` (`StorageDisaggregatedColumnar.cpp:1098-1136`) uses `newThreadManager()->scheduleThenDetach(true, "PrefetchRNColumnarReader", ...)` — a detached thread outside the pipeline scheduler

### 3.4 What already works correctly

- `RNColumnarSourceOp` already extends `SourceOp` → correct base class
- `readImpl()` already handles DONE and t_block output correctly
- `executeIOImpl()` already creates `RNColumnarInputStream` per reader work and reads blocks
- `tryAcquireReaderWork()` already does work-stealing from shared queue
- Region error replan (`replaceReaderWork`) already works correctly
- The stream path (`RNColumnarInputStream`) is independent and unaffected

## 4. Feasibility Assessment

### What already exists
| Capability | Status |
|---|---|
| `PipeConditionVariable` with `registerTask`/`notifyAll` | ✅ |
| `NotifyFuture` interface + `current_notify_future` thread-local | ✅ |
| `WAIT_FOR_NOTIFY` in `OperatorStatus` | ✅ |
| `WAIT_ON_TABLE_SCAN_READ` in `NotifyType` | ✅ |
| `NullSourceOp` for empty scans | ✅ |
| `addInboundIOProfileInfos` / `addOperatorProfileInfos` in DAGContext | ✅ |
| `SourceOp` base with `readImpl`/`awaitImpl`/`executeIOImpl`/`operateSuffixImpl` | ✅ |

### Conflicts with the proposal
1. **`getOrCreateReader` is shared between stream and pipeline paths**: The blocking behavior must be preserved for the stream path. Solution: rename current method to `getOrCreateReaderBlocking()`, add new `tryGetReadyReader()` for pipeline.

2. **`RNColumnarReaderWork` currently uses `std::condition_variable`**: Must be replaced with `PipeConditionVariable`. Since `PipeConditionVariable` is not a `NotifyFuture`, we need an adapter.

3. **`RNColumnarSourceOp` currently has `current_input_stream` as `BlockInputStreamPtr`**: In the new design, the source should own the reader work directly, materializing via `executeIOImpl()` (for NotStarted → Creating transition) or waiting via `WAIT_FOR_NOTIFY` (for Creating → Ready). The `RNColumnarInputStream` wrapper should be retained only for the actual FFI read + deserialize inside `executeIOImpl()`.

### What must be clarified first
None — the design doc is sufficiently detailed and the code matches the described baseline.

### Rollout assumptions
- `ENABLE_NEXT_GEN_COLUMNAR == 1` (compile-time flag)
- `context.getSharedContextDisagg()->use_columnar == true` (runtime flag)
- Both flags must be true for the changed code to execute

## 5. Scope

### In Scope
- Code split: move `RNColumnarSourceOp` to dedicated `ColumnarSourceOp.h/.cpp`
- Replace `std::condition_variable` with `PipeConditionVariable` in `RNColumnarReaderWork`
- Add `tryGetReadyReader()` non-blocking method to `RNColumnarReadTask`
- Implement `WAIT_FOR_NOTIFY` path in `RNColumnarSourceOp::awaitImpl()`
- Add profile recording (`addInboundIOProfileInfos`, `addOperatorProfileInfos`) for table scan
- Add `NullSourceOp` for empty reader scenarios
- Add `RNColumnarReadTask::cancel()` and wire through suffix/destructor
- State machine convergence in `RNColumnarSourceOp`
- Unit tests for state transitions, notify, empty scan, profile recording, cancel

### Out of Scope
- Columnar helper FFI protocol changes
- DAGRequest / table scan schema / filter pushdown semantic changes
- New reader split algorithms
- Replacing the stream model (`RNColumnarInputStream` path still works for non-pipeline)
- Changing `PipeConditionVariable` interface
- TiFlash-write disaggregated path
- Non-disaggregated compute mode
- `ENABLE_NEXT_GEN_COLUMNAR == 0` paths

## 6. Module Split

### Module A: `ColumnarSourceOp` (new files)
**Responsibility**: Pipeline source operator for columnar reads
**Files**:
- `dbms/src/Storages/Columnar/ColumnarSourceOp.h` (new)
- `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp` (new)
- `dbms/CMakeLists.txt` — add `add_headers_and_sources(dbms src/Storages/Columnar)` after the `src/Storages` line (line 88)

**Contains**: `RNColumnarSourceOp` class, `RNColumnarReaderNotifyFuture`
**Why isolated**: The source operator, state machine, and notify logic are a self-contained concern. They depend on `RNColumnarReadTask`/`RNColumnarReaderWork` interfaces but not on planning or FFI details.

### Module B: `RNColumnarReaderWork` notify infrastructure (in existing files)
**Responsibility**: Replace `std::condition_variable` with `PipeConditionVariable`, add `NotifyFuture` adapter
**Files**:
- `dbms/src/Storages/StorageDisaggregatedColumnar.h` — modify `RNColumnarReaderWork`
- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — add `tryGetReadyReader()`, modify `getOrCreateReader` → `getOrCreateReaderBlocking`, modify `prefetchReaderWork`

### Module C: Profile & empty reader (in existing files)
**Responsibility**: Profile recording and NullSourceOp
**Files**:
- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — modify `readThroughColumnar` (pipeline overload)

### Module D: Cancel semantics (in existing files)
**Responsibility**: Cancel, error propagation, resource cleanup
**Files**:
- `dbms/src/Storages/StorageDisaggregatedColumnar.h` — add `cancel()` to `RNColumnarReadTask`
- `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — implement cancel, wire suffix
- `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp` — suffix cancel logic

### Sequencing constraints
```
Module A (code split) → Module B (notify) → Module C (profile/empty) can be done in parallel with B
Module B (notify) → Module D (state machine) → Module E (cancel)
```

Actually, profile/empty (Module C) has no dependency on notify (Module B), so they can be sequenced as:
```
Commit 1: Module A (code split, pure mechanical)
Commit 2: Module C (profile + empty, no behavioral change to source op)
Commit 3: Module B (notify infrastructure + WAIT_FOR_NOTIFY path)
Commit 4: State machine convergence
Commit 5: Module D (cancel + error)
```

## 7. Interface Plan

### 7.1 `RNColumnarReaderWork` changes

**Before:**
```cpp
struct RNColumnarReaderWork {
    RNColumnarReaderPlan plan;
    std::mutex mutex;
    std::condition_variable cv;  // ← replaced
    RNColumnarReaderMaterializeState state;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};
```

**After:**
```cpp
struct RNColumnarReaderNotifyFuture : public NotifyFuture {
    void registerTask(TaskPtr && task) override {
        task->setNotifyType(NotifyType::WAIT_ON_TABLE_SCAN_READ);
        pipe_cv.registerTask(std::move(task));
    }
    void notifyAll() { pipe_cv.notifyAll(); }
    PipeConditionVariable pipe_cv;
};

struct RNColumnarReaderWork {
    RNColumnarReaderPlan plan;
    std::mutex mutex;
    RNColumnarReaderNotifyFuture notify_future;  // ← new
    RNColumnarReaderMaterializeState state;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};
```

**Rationale**: `RNColumnarReaderNotifyFuture` implements the `NotifyFuture` interface so it can be set as `current_notify_future` via `setNotifyFuture()`. The `PipeConditionVariable` handles the task queue and wakeup. The `NotifyType` is set to `WAIT_ON_TABLE_SCAN_READ` which already has metrics support.

### 7.2 `RNColumnarReadTask` new methods

```cpp
// Non-blocking: returns reader if Ready, throws if Failed/Consumed, returns nullopt if Creating/NotStarted
std::optional<ColumnarReaderPtr> tryGetReadyReader(const RNColumnarReaderWorkPtr & reader_work);

// Blocking (renamed, for stream path only): same as current getOrCreateReader
ColumnarReaderPtr getOrCreateReaderBlocking(const RNColumnarReaderWorkPtr & reader_work);

// Cancel all pending/creating works and wake waiting tasks
void cancel(const String & reason);
```

### 7.3 `RNColumnarSourceOp` state machine

```cpp
enum class ColumnarSourceState {
    READY_BLOCK,   // t_block has a block ready
    NEED_READER,   // no current reader work, need to acquire one
    READING,       // have a reader, can read next block
    WAIT_READER,   // reader work is being materialized (Creating state)
    DONE,          // all reader works consumed
};
```

State transitions:
```
NEED_READER → tryAcquireReaderWork()
  → nullopt: DONE
  → NotStarted: NEED_READER (return IO_IN, executeIOImpl materializes inline)
  → Creating: WAIT_READER (return WAIT_FOR_NOTIFY)
  → Ready: READING (consume reader, return IO_IN or read block)

READING → executeIOImpl() → fn_read_block + deserialize
  → block: READY_BLOCK
  → empty: release reader, NEED_READER

READY_BLOCK → readImpl() → emit block → NEED_READER or READING

WAIT_READER → awaitImpl() → check state without blocking
  → Ready: READING
  → Failed: throw
  → Creating: stay in WAIT_READER, return WAIT_FOR_NOTIFY

DONE → readImpl() → emit empty block, HAS_OUTPUT
```

## 8. Implementation Steps (ordered)

### Step 1: Verify existing tests pass
Run existing columnar/disaggregated tests to establish baseline before any change.

### Step 2: Commit 1 — Code split
Mechanically move `RNColumnarSourceOp` from `StorageDisaggregatedColumnar.*` to `ColumnarSourceOp.{h,cpp}`. No behavior change. Verify compilation.

### Step 3: Commit 2 — Profile recording + empty reader
Add `NullSourceOp` and profile recording in `readThroughColumnar` (pipeline overload). Verify with unit tests + EXPLAIN ANALYZE.

### Step 4: Commit 3 — NotifyFuture infrastructure
Add `RNColumnarReaderNotifyFuture`, replace `std::condition_variable`, add `tryGetReadyReader()`, implement `WAIT_FOR_NOTIFY` in `awaitImpl()`. Verify with notify unit tests.

### Step 5: Commit 4 — State machine convergence
Add explicit `ColumnarSourceState` enum, ensure each method respects its boundary. Verify with state transition unit tests.

### Step 6: Commit 5 — Cancel and error handling
Add `cancel()`, wire suffix/destructor, add cancel check in prefetch thread. Verify with cancel tests.

### Step 7: Full integration validation
Run all columnar/disaggregated/pipeline integration tests.

## 9. Commit Plan

### Commit 1: `columnar: extract RNColumnarSourceOp to dedicated files`

**Scope**: Pure code movement
- New: `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
- New: `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.h` — remove `RNColumnarSourceOp` class body, add `#include "Storages/Columnar/ColumnarSourceOp.h"` at end (after the class forward-declares used by other code)
- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — remove `RNColumnarSourceOp` method implementations
- Modify: `dbms/CMakeLists.txt` — add one line after line 88 (`add_headers_and_sources(dbms src/Storages)`): `add_headers_and_sources(dbms src/Storages/Columnar)`

**Why independent**: No behavioral change. The class is simply moved. Compilation success proves correctness.

**Validation**: `ninja <columnar target>` compiles. Existing gtest for columnar/disaggregated passes.

---

### Commit 2: `columnar: fix pipeline profile recording and empty-reader handling`

**Scope**: Fix two independent bugs in pipeline columnar path
- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — `readThroughColumnar` pipeline overload
  - Handle `read_columnar_tasks.empty()` or `getReaderCount() == 0`: build `NullSourceOp` with correct header from `genNamesAndTypesForTableScan`
  - After adding `RNColumnarSourceOp` (or `NullSourceOp`), call `addInboundIOProfileInfos(table_scan_id, ...)` and `addOperatorProfileInfos(table_scan_id, ...)`
  - Add `#include <Operators/NullSourceOp.h>` and `#include <Flash/Coprocessor/DAGContext.h>` (if not already)

**Why independent**: Profile recording and empty reader are self-contained fixes that don't change operator behavior. They use existing patterns from `buildRemoteSegmentSourceOps` and `PhysicalExchangeReceiver`.

**Validation**:
- New gtest: empty table → NullSourceOp created, `getCurrentHeader()` succeeds
- New gtest: `DAGContext::inbound_io_profile_infos_map[table_scan_id]` exists after build
- New gtest: `DAGContext::operator_profile_infos_map[table_scan_id]` points to source profile, not projection/filter

---

### Commit 3: `columnar: replace blocking reader wait with NotifyFuture`

**Scope**: Add NotifyFuture infrastructure, implement WAIT_FOR_NOTIFY path
- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.h`
  - Add `RNColumnarReaderNotifyFuture` struct (inherits `NotifyFuture`, holds `PipeConditionVariable`)
  - Replace `std::condition_variable cv` in `RNColumnarReaderWork` with `RNColumnarReaderNotifyFuture notify_future`
  - Add `tryGetReadyReader()` declaration to `RNColumnarReadTask`
  - Rename `getOrCreateReader()` → `getOrCreateReaderBlocking()` (keep for stream path)
  - Add `#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>` and `PipeConditionVariable.h`

- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`
  - Implement `tryGetReadyReader()`: non-blocking state check
    - `Ready` → move reader out, set Consumed, return reader
    - `Failed` → rethrow exception
    - `Consumed` → throw LOGICAL_ERROR
    - `Creating` → return `std::nullopt`
    - `NotStarted` → return `std::nullopt`
  - Rename existing `getOrCreateReader` → `getOrCreateReaderBlocking`
  - Update `prefetchReaderWork()`: replace `reader_work->cv.notify_all()` with `reader_work->notify_future.notifyAll()`
  - Update `getOrCreateReaderBlocking()`: replace `cv.wait()` with polling approach (or keep `cv` only in blocking path — see note)

- Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
  - `awaitImpl()`:
    ```cpp
    if (current_reader_work) {
        auto reader = task->tryGetReadyReader(current_reader_work);
        if (reader.has_value()) {
            // Reader is ready, create input stream, proceed to read
            current_input_stream = task->createInputStream(current_reader_work);
            return OperatorStatus::IO_IN;
        }
        // Reader is still Creating — wait for notify
        setNotifyFuture(&current_reader_work->notify_future);
        return OperatorStatus::WAIT_FOR_NOTIFY;
    }
    // No current work, try to acquire next
    auto next_work = task->tryAcquireReaderWork();
    if (!next_work.has_value()) { done = true; return HAS_OUTPUT; }
    current_reader_work = next_work.value();
    // Check state: if NotStarted, return IO_IN for executeIOImpl to materialize
    // if Creating, WAIT_FOR_NOTIFY
    ```

**Design note — blocking path**: The stream path (`RNColumnarInputStream::ensureReader()`) still calls `getOrCreateReaderBlocking()`. To avoid maintaining two condition variable mechanisms, we can keep the old `std::condition_variable cv` ONLY for the blocking path, or use a polling loop with `PipeConditionVariable`. Recommendation: keep a minimal `std::condition_variable` in the blocking helper only (not in `RNColumnarReaderWork`). The blocking helper wraps `tryGetReadyReader()` with a poll+sleep or uses a separate sync mechanism. Alternatively, rename `getOrCreateReaderBlocking` to call `tryGetReadyReader` in a loop with `notify_future` wait — simplest approach.

**Why independent**: This commit changes only the wait mechanism. The source behavior (what blocks are produced, in what order) is unchanged. The stream path is preserved.

**Validation**:
- New gtest: `RNColumnarReaderWork` notify — prefetch completes → task wakes up
- New gtest: `tryGetReadyReader()` returns nullopt when `Creating`, returns reader when `Ready`
- New gtest: `awaitImpl()` returns `WAIT_FOR_NOTIFY` when reader is `Creating`
- Existing integration tests: columnar pipeline table scan with `use_columnar=1`

---

### Commit 4: `columnar: converge RNColumnarSourceOp state machine`

**Scope**: Add explicit state tracking, enforce method boundaries
- Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
  - Add `ColumnarSourceState` enum
  - Add state member, replace implicit state checks (t_block.has_value(), done, current_input_stream != null) with explicit state

- Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
  - `readImpl()`: only check state and emit block, call `awaitImpl()` for non-ready states. NO FFI calls.
  - `awaitImpl()`: only check reader work state without blocking. NO allocation, NO FFI. Return `HAS_OUTPUT`/`IO_IN`/`WAIT_FOR_NOTIFY`.
  - `executeIOImpl()`: materialize reader (if NotStarted), read one block via FFI + deserialize, cache in t_block, return `HAS_OUTPUT`. At most one block per call.

**Why independent**: This is a pure refactoring of the operator's internal state tracking. External behavior is unchanged from Commit 3.

**Validation**:
- New gtest: state transitions for all 5 states
- New gtest: `executeIOImpl()` produces exactly one block per call
- Existing integration tests pass

---

### Commit 5: `columnar: add cancel and error propagation to columnar pipeline`

**Scope**: Cancel semantics, error handling, resource cleanup
- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.h`
  - Add `void cancel(const String & reason)` to `RNColumnarReadTask`
  - Add `bool cancelled` flag

- Modify: `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`
  - `RNColumnarReadTask::cancel(reason)`:
    - Lock `pending_reader_works_mutex`, clear queue, set cancelled flag
    - For each work in any state:
      - `NotStarted`/`Creating` → set state to `Failed`, store exception, `notifyAll()`
      - `Ready`/`Consumed` → no action (already done or being consumed)
  - `prefetchReaderWork()`: check cancelled flag before materializing reader
  - `tryAcquireReaderWork()`: check cancelled flag

- Modify: `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
  - `operateSuffixImpl()`: if not done, call `task->cancel("query cancelled")`
  - Destructor (if needed): ensure cancel on premature destruction
  - `awaitImpl()`/`executeIOImpl()`: check `Failed`/`Cancelled` state → throw

**Why independent**: Cancel is an additive feature. Normal execution path is unaffected.

**Validation**:
- New gtest: cancel while reader is Creating → task wakes up with error
- New gtest: cancel after all reads complete → no-op
- New gtest: prefetch thread sees cancelled flag and stops

## 10. Validation Strategy

### Unit Tests (gtest)

| Test | Commit | What it verifies |
|---|---|---|
| `ColumnarSourceOp_EmptyReader_NullSourceOp` | 2 | Empty reader → NullSourceOp created, header valid |
| `ColumnarSourceOp_Profile_TableScanRecorded` | 2 | `inbound_io_profile_infos_map[table_scan_id]` and `operator_profile_infos_map[table_scan_id]` exist |
| `ColumnarSourceOp_Profile_NotOverwrittenByProjection` | 2 | Source profile stays as table scan, not overwritten by later ops |
| `ColumnarReaderWorkNotify_WakeOnPrefetchComplete` | 3 | Task registered → prefetch completes → task woken and resubmitted |
| `ColumnarReaderWorkNotify_TryGetReadyReader_Creating` | 3 | `tryGetReadyReader()` returns nullopt when state is Creating |
| `ColumnarReaderWorkNotify_TryGetReadyReader_Ready` | 3 | `tryGetReadyReader()` returns reader when state is Ready |
| `ColumnarSourceOp_AwaitImpl_ReturnsWaitForNotify` | 3 | `awaitImpl()` returns `WAIT_FOR_NOTIFY` when reader is Creating |
| `ColumnarSourceOp_StateTransitions` | 4 | All valid state transitions: READY_BLOCK→..., NEED_READER→..., etc. |
| `ColumnarSourceOp_ExecuteIO_OneBlockPerCall` | 4 | `executeIOImpl()` returns HAS_OUTPUT after exactly one block |
| `ColumnarSourceOp_ReadImpl_NoFFI` | 4 | `readImpl()` never calls columnar FFI (verify via mock) |
| `ColumnarReaderWork_RegionErrorReplan` | 3 | Failed work replaced, new works in pending_reader_works, notify preserved |
| `ColumnarSourceOp_Cancel_WakesWaitingTask` | 5 | Cancel during Creating → task wakes, exception propagated |
| `ColumnarSourceOp_Cancel_AfterCompletion_NoOp` | 5 | Cancel after all reads → no crash, no double-free |
| `ColumnarSourceOp_PrefetchThread_StopsOnCancel` | 5 | Prefetch checks cancelled flag, does not materialize reader |

### Integration Tests

- Disaggregated compute mode + `use_columnar=1` + pipeline executor → table scan produces correct results
- Table scan with generated columns → placeholder filled correctly
- Table scan with timestamp/duration cast → values cast correctly
- Table scan with pushed-down filter / late-materialization filter → TiFlash-side re-filtering correct
- Partition table scan → `_tidb_tid` / extra table id column correct
- Region miss / lock / PD error → retry path works, eventual success

### Runtime Checks

- `PipelineExecutor::toString()` shows table scan in pipeline
- `EXPLAIN ANALYZE` shows table scan runtime stats (rows, bytes, execution time)
- `tiflash_pipeline_task_change_to_status` metrics show `IO_IN` and `WAIT_FOR_NOTIFY` transitions
- `tiflash_pipeline_wait_on_notify_tasks` metric `type_wait_on_table_scan_read` increments/decrements correctly
- IO worker not blocked for long periods under high concurrency

### Environment Limitations

- Columnar helper FFI requires a real or mocked kvengine backend. Unit tests may need mock FFI functions.
- Region error replan tests may need mock region cache.
- If mock infrastructure doesn't exist, unit tests should focus on state transitions and notify mechanics without FFI calls.

## 11. Risks And Open Questions

### Accepted Risks

| Risk | Mitigation |
|---|---|
| Missed wakeup on WAIT_FOR_NOTIFY | `tryGetReadyReader()` and `setNotifyFuture()` must be called under the same mutex; after setting the notify future, re-check state before returning (standard condition-variable protocol) |
| Profile recording timing wrong | Record immediately after `addConcurrency()` for source ops, before any transform ops are added. This matches `buildRemoteSegmentSourceOps` pattern. |
| Cancel vs prefetch thread race | `RNColumnarReaderWork` owns the reader ptr; state switch to Failed/Cancelled under mutex. Only one consumer moves reader out. Prefetch thread checks state before writing. |
| Region error replan concurrent with source acquire | `replaceReaderWork()` only rewrites current failed work and inserts new works into `pending_reader_works` under lock. Source acquires works from the same queue under the same lock. |
| WAIT_FOR_NOTIFY metrics type | Use existing `WAIT_ON_TABLE_SCAN_READ` — already has metrics in `PipeConditionVariable::updateTaskNotifyWaitMetrics`. |

### Open Questions

1. **Should `PipeConditionVariable` be made to implement `NotifyFuture` directly?**
   - Pro: Simpler, no adapter needed. `PipeConditionVariable` already has `registerTask()`.
   - Con: `PipeConditionVariable` is used in many places (shared queue, spill bucket, join build, etc.) without `NotifyFuture`. Adding inheritance might have unintended side effects.
   - **Recommendation**: Use the adapter approach (`RNColumnarReaderNotifyFuture`) to minimize blast radius.

2. **Should the blocking `getOrCreateReaderBlocking()` keep `std::condition_variable` or use `PipeConditionVariable`?**
   - If we remove `std::condition_variable cv` from `RNColumnarReaderWork`, the blocking path can't use it.
   - **Recommendation**: The blocking path can poll `tryGetReadyReader()` with wait on `PipeConditionVariable`. Since `PipeConditionVariable::registerTask` requires a `TaskPtr`, and the stream path doesn't have tasks, we need either: (a) keep `std::condition_variable` in work for blocking path only, or (b) use a separate synchronization primitive for blocking path.
   - **Current plan**: Keep a `std::condition_variable` for the blocking path, alongside the `PipeConditionVariable` for the pipeline path. Or, simpler: have `getOrCreateReaderBlocking()` do a timed poll with sleep — acceptable since stream path is legacy.

3. **Where should `ColumnarSourceOp` files live?**
   - Design doc suggests `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
   - Verified: `dbms/src/Storages/Columnar/` does NOT exist yet.
   - The build system (`cmake/dbms_glob_sources.cmake`) uses `add_headers_and_sources(dbms src/Storages/Columnar)` in `dbms/CMakeLists.txt` (near line 89). No additional `CMakeLists.txt` needed in the new directory — just one line appended.
   - **Decision**: Create `dbms/src/Storages/Columnar/` directory, place `ColumnarSourceOp.h` and `ColumnarSourceOp.cpp` there, and add `add_headers_and_sources(dbms src/Storages/Columnar)` to `dbms/CMakeLists.txt`. This keeps columnar source files alongside the columnar storage logic in `StorageDisaggregatedColumnar.*`.

4. **What is the correct header for `NullSourceOp` in empty-reader case?**
   - `NullSourceOp` already builds its own header from the `Block` passed to constructor.
   - For empty columnar read, the header should be `genNamesAndTypesForTableScan(table_scan)` → matches what `PhysicalTableScan::build()` uses.
   - **Answer**: `Block header(getColumnWithTypeAndName(genNamesAndTypesForTableScan(table_scan)))` — matches design doc.

5. **How to handle `RNColumnarInputStream` creation inside `executeIOImpl`?**
   - Currently, `executeIOImpl()` creates an `RNColumnarInputStream` wrapping the reader work, then calls `read()` on it to get one block.
   - After Commit 3, `executeIOImpl()` should:
     - If reader work is `NotStarted`: call `createColumnarReaderWithBackoff()` to materialize, set state to `Ready` (for consistency, though only this source uses it)
     - Call `fn_read_block()` + deserialize directly (or keep creating `RNColumnarInputStream` temporarily)
   - **Recommendation**: Keep `RNColumnarInputStream` as the block-read helper inside `executeIOImpl()` to minimize change surface. The input stream is created with a fixed reader work (not shared), so each `readImpl()` call returns one block. This is already the pattern.

## 12. Appendix: Key Code Relationships

```
readThroughColumnar (pipeline overload)
  │
  ├─ buildColumnarReadTaskWithBackoff()  → RNColumnarReadTask (shared)
  │    └─ pending_reader_works (deque<RNColumnarReaderWorkPtr>)
  │
  ├─ [Commit 2] addInboundIOProfileInfos / addOperatorProfileInfos
  ├─ [Commit 2] NullSourceOp (if empty)
  │
  ├─ addConcurrency(RNColumnarSourceOp) × source_num
  │    └─ [Commit 3-5] awaitImpl → WAIT_FOR_NOTIFY → NotifyFuture
  │    └─ [Commit 3-5] executeIOImpl → fn_read_block + deserialize
  │    └─ [Commit 4] explicit state machine
  │    └─ [Commit 5] cancel on suffix
  │
  ├─ executeGeneratedColumnPlaceholder
  ├─ extraCast
  └─ filterConditionsWithPushedDownFilters
```
