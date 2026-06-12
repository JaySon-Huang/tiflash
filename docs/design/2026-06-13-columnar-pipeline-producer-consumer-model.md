# StorageDisaggregated Columnar: 生产者-消费者流水线读取模型

* Author(s): JaySon-Huang
* Date: 2026-06-13
* Related docs:
  * [2026-06-09-storage-disaggregated-columnar-pipeline.md](./2026-06-09-storage-disaggregated-columnar-pipeline.md)
  * [2026-06-09-storage-disaggregated-columnar-pipeline-impl.md](./2026-06-09-storage-disaggregated-columnar-pipeline-impl.md)

## Summary

当前 `RNColumnarSourceOp` 的 pipeline 模型是"串行乒乓"：每次调用 `executeIOImpl()` 读取并反序列化一个 block，返回 `HAS_OUTPUT` 后走完整的 CPU transform 链（generated column placeholder → cast → filter → projection），然后再回到 IO pool 读下一个 block。同一个 pipeline task 内 IO 和 CPU 严格串行，无法利用流水线重叠。

本文提议将 columnar 读取路径改为 **生产者-消费者流水线模型**：新增 `ColumnarReadSourceOp` 作为 IO producer source，持续 materialize reader、读取 block、反序列化列，并通过现有 `SharedQueueSinkOp` 推入有界 `SharedQueue`；consumer group 使用 `SharedQueueSourceOp`（或一个很薄的 `ColumnarQueueSourceOp` wrapper）从队列 pop block，再执行 generated column placeholder → cast → filter → projection。两个 pipeline group 通过 `SharedQueue` 连接，实现 IO 读取与 CPU transform 的并行执行。

## Context

### 当前模型（lost wakeup race 修复后）

reader materialize 已改为在 IO pool 中内联执行（`awaitImpl()` 对 NotStarted/Creating 均返回 `IO_IN`，由 `executeIOImpl()` 内联 materialize）。详见[设计文档 §Observed Issue](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race)。

但 **block 读取仍然是串行的**：

```text
同一个 Pipeline Task:
  CPU: awaitImpl() → IO_IN
  IO:  executeIOImpl() → fn_read_block + deserialize → HAS_OUTPUT
  CPU: readImpl() → swap block → downstream transform ops → Sink
  CPU: awaitImpl() → IO_IN
  IO:  executeIOImpl() → fn_read_block + deserialize → HAS_OUTPUT
  ...
```

关键约束：IO 和 CPU 工作绑定在同一 task 上，**后续 block 的读取必须等前一个 block 走完整条 transform 链**。

### 参考模型：ExchangeReceiver

`ExchangeReceiverSourceOp` 已经是生产者-消费者模型：

```text
gRPC 网络线程 (生产者，在 pipeline 之外):
  → ReceivedMessageQueue::push(packet)

ExchangeReceiverSourceOp (消费者，在 pipeline 内):
  → tryReceive() → decode → block_queue.push()
  → readImpl() → pop from block_queue → HAS_OUTPUT → 下游 transform 链
```

生产者（gRPC 线程）和消费者（pipeline task）完全解耦，互不阻塞。

`SharedQueue` 基础设施已存在（`Operators/SharedQueue.h`），且在 `executeUnion`、`executeMppExpand` 等场景中通过 `PipelineExecGroupBuilder::addGroup()` + `SharedQueueSinkOp` / `SharedQueueSourceOp` 实现了多 group 连接。

### 为什么当前串行模型在分析型查询中存在性能瓶颈

假设 `fn_read_block + deserialize = 10ms/block`，`filter + cast + projection = 8ms/block`，则：

```text
串行(当前): |==10ms IO==|==8ms CPU==|==10ms IO==|==8ms CPU==| = 36ms for 2 blocks
并行(提议): |==10ms IO==|==10ms IO==| = 20ms IO 并行
            |==8ms CPU==|==8ms CPU==| = 16ms CPU 并行
            总耗时 ≈ max(20, 16) ≈ 20ms  →  约 44% 延迟改善
```

对于 filter / projection 较重的分析型查询，流水线重叠能带来显著的吞吐提升。

## Goals

* 将 `fn_read_block` + 列反序列化从 `RNColumnarSourceOp::executeIOImpl()` 中分离出来，放入独立的 `ColumnarReadSourceOp`，在 IO pool 中执行并每次最多产出一个 block
* consumer group 通过 `SharedQueueSourceOp` 或 `ColumnarQueueSourceOp` 从有界 `SharedQueue` pop block，只在 CPU pool 中执行轻量状态检查
* 通过 `SharedQueue` 的长度上限实现反压：IO 过快时 `SharedQueueSinkOp` 返回 `WAIT_FOR_NOTIFY`，释放 task；CPU 消费不足时 `SharedQueueSourceOp` / `ColumnarQueueSourceOp` 返回 `WAIT_FOR_NOTIFY`
* 保持与现有 generated column placeholder → extra cast → filter → projection 的顺序兼容性
* 保持与 stream 路径（`RNColumnarInputStream`）的行为一致，不改变 stream 路径
* 正确归档 table scan 的 source profile 和 inbound IO profile

## Non-Goals

* 不改变 columnar helper 的 FFI 协议
* 不改变 columnar reader 的 FFI 生命周期和 read block 协议
* 不在本设计中恢复 detached prefetch thread + `WAIT_FOR_NOTIFY` 的 reader materialize 模式；该模式已被 Observed Issue 证明存在 lost wakeup race
* 不改变 reader work 的基本分配策略（共享 `RNColumnarReadTask` work queue）；但 producer model 应避免同一个 reader work 被 detached prefetch 和 inline materialize 同时创建
* 不改变 TiDB 下发的 DAGRequest、table scan schema 或 filter pushdown 语义
* 不让多个线程并发读取同一个 `ColumnarReaderPtr`；不同 reader work 可由不同 producer source 并发处理
* 不为非 pipeline 路径实现此模型

## Design

### 架构概览

```text
readThroughColumnar (pipeline overload)
  │
  ├─ [Group 1] IO Producer Group (concurrency = producer_num)
  │     ColumnarReadSourceOp (SourceOp)
  │       ├─ awaitImpl() → reader NotStarted/Creating -> IO_IN
  │       ├─ executeIOImpl() → materialize reader + fn_read_block + deserialize
  │       └─ readImpl() → emit ready block
  │     SharedQueueSinkOp
  │       └─ writeImpl() → tryPush to SharedQueue → full? WAIT_FOR_NOTIFY
  │
  ├─ SharedQueue (bounded, cap = 2~4 blocks)
  │     ┌───┬───┬───┬───┐
  │     │   │   │   │   │
  │     └───┴───┴───┴───┘
  │       ↑ push        ↓ pop
  │
  ├─ [Group 2] CPU Consumer Group (concurrency = source_num)
  │     SharedQueueSourceOp / ColumnarQueueSourceOp (SourceOp)
  │       → readImpl() → tryPop from SharedQueue → empty? WAIT_FOR_NOTIFY
  │       → 有 block → HAS_OUTPUT → 下游 transform 链
  │
  ├─ executeGeneratedColumnPlaceholder
  ├─ extraCast
  ├─ filterConditionsWithPushedDownFilters
  └─ addColumnarTableScanProfileInfos
```

### 关键组件

#### ColumnarReadSourceOp (新增 SourceOp)

```cpp
class ColumnarReadSourceOp : public SourceOp
{
public:
    ColumnarReadSourceOp(
        PipelineExecutorContext & exec_context,
        const String & req_id,
        RNColumnarReadTaskPtr task);

protected:
    OperatorStatus readImpl(Block & block) override; // emit cached block or EOF
    OperatorStatus awaitImpl() override;      // reader NotStarted/Creating → IO_IN
    OperatorStatus executeIOImpl() override;  // fn_read_block + deserialize

private:
    RNColumnarReadTaskPtr task;
    BlockInputStreamPtr current_input_stream;
    std::optional<Block> t_block;
    RNColumnarReaderWorkPtr current_reader_work;
    // ... reader work management, reuse the current inline materialize state machine
};
```

**职责**：
- 管理 reader work 的生命周期（acquire → inline materialize 或消费 ready reader → read）
- 在 `executeIOImpl()` 中内联 materialize reader，并调用 `fn_read_block` + 列反序列化，产出一个 block
- `readImpl()` 只负责把 `t_block` 交给下游 `SharedQueueSinkOp`
- reader 读完所有 block → reset → acquire next reader work → 循环

`ColumnarReadSourceOp` 后面直接接现有 `SharedQueueSinkOp`。这是为了符合当前 `PipelineExecBuilder` 的 contract：每条 pipeline exec 必须是 `SourceOp -> TransformOp* -> SinkOp`，不能只有一个自驱动 sink。

**并发度**：建议使用 `producer_num = min(source_num, reader_count)`，也可以先通过配置限制为 1 做保守落地。约束是不能让多个线程并发读取同一个 `ColumnarReaderPtr`；但不同 reader work 拥有不同 reader，可以由不同 producer source 并发处理。若先固定为 1，需要在文档和实现中明确这是保守开关，不是 reader 非线程安全推出的必然限制。

#### SharedQueueSourceOp / ColumnarQueueSourceOp (consumer)

consumer group 可以直接复用 `SharedQueueSourceOp`。如果需要保留 `RNColumnarSourceOp` 名字用于 profile/name 兼容，也应将它重构成一个很薄的 queue source wrapper，不再持有 reader work，也不再实现 `executeIOImpl()` / `awaitImpl()`。

```cpp
OperatorStatus ColumnarQueueSourceOp::readImpl(Block & block)
{
    // 从 SharedQueue pop block，类似 ExchangeReceiverSourceOp
    auto result = shared_queue_source_holder->tryPop(block);
    switch (result) {
    case MPMCQueueResult::OK:
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::EMPTY:
        setNotifyFuture(shared_queue_source_holder.get());
        return OperatorStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::FINISHED:
        block = {};  // EOF
        return OperatorStatus::HAS_OUTPUT;
    case MPMCQueueResult::CANCELLED:
        throw Exception("query cancelled");
    }
}
```

**不再需要** reader materialize / read block 相关的 `executeIOImpl()` 和 `awaitImpl()`（所有 IO 逻辑移到 `ColumnarReadSourceOp`）。queue empty/full 的等待由 `SharedQueueSourceOp` / `SharedQueueSinkOp` 使用 `WAIT_FOR_NOTIFY` 处理。

#### SharedQueue 配置

```cpp
// cap = 2~4 blocks，折中 IO/CPU 流水线重叠与内存占用
SharedQueue::build(
    exec_context,
    /*producer=*/producer_num,
    /*consumer=*/source_num,
    /*max_buffered_bytes=*/-1,  // 不限字节数，仅限 block 数
    /*max_queue_size=*/4
);
```

`SharedQueue` 基于 `LooseBoundedMPMCQueue<Block>`，`tryPush` 满时返回 `MPMCQueueResult::FULL`，`tryPop` 空时返回 `MPMCQueueResult::EMPTY`。

### 反压流程

```text
场景 A: IO 快于 CPU (queue full):
  SharedQueueSinkOp::writeImpl()
    → tryPush → FULL
    → setNotifyFuture(shared_queue_sink_holder)
    → WAIT_FOR_NOTIFY ──► 释放 producer pipeline task
  consumer source 消费后 → shared_queue notify write waiters → producer pipeline 被唤醒 → 继续 push

场景 B: CPU 快于 IO (queue empty):
  SharedQueueSourceOp / ColumnarQueueSourceOp::readImpl()
    → tryPop → EMPTY
    → setNotifyFuture(shared_queue_source_holder)
    → WAIT_FOR_NOTIFY ──► 释放 consumer pipeline task
  producer push 后 → shared_queue notify read waiters → consumer source 被唤醒 → 继续 pop

场景 C: producer EOF (所有 reader work 读完):
  ColumnarReadSourceOp 发空 block → SharedQueueSinkOp::writeImpl() 返回 FINISHED
  SharedQueueSinkOp 析构 / finish → shared_queue.producerFinish()
  SharedQueueSourceOp::tryPop → FINISHED → 发空 block → 下游正常结束
```

### SharedQueue WAIT_FOR_NOTIFY 的安全性

本方案中 producer side 的 `SharedQueueSinkOp` 和 consumer side 的 `SharedQueueSourceOp` / `ColumnarQueueSourceOp` 都使用了 `WAIT_FOR_NOTIFY`，但其安全性条件与 columnar detached thread 的 `WAIT_FOR_NOTIFY` 有根本区别。

[设计文档 §Observed Issue](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race) 详细分析了 columnar reader materialize 中 `WAIT_FOR_NOTIFY` 的 lost wakeup race：因为 producer（detached Prefetch thread）是一次性的——每个 reader work 只 materialize 一次，`notifyAll` 只调用一次——如果这次调用时 waiter 尚未注册到 `pipe_cv`，wakeup 永久丢失。

`SharedQueue` 的 `WAIT_FOR_NOTIFY` 不会产生此 race，原因有两个：

1. **Producer 和 consumer 都是 pipeline task**。`ColumnarReadSourceOp -> SharedQueueSinkOp` 只要还有 reader work 可读，就会继续 push block。`SharedQueueSourceOp` / `ColumnarQueueSourceOp` 只要下游还需要数据，就会继续 pop。它们的等待对象是队列状态，而不是一次性 reader materialize 结果。

2. **`SharedQueue` 自身维护 read/write waiter 和 finish/cancel 状态**。`SharedQueueSinkHolder::registerTask()` 注册 write waiter，`SharedQueueSourceHolder::registerTask()` 注册 read waiter；`tryPush` / `tryPop` 改变队列状态时会唤醒对侧，`producerFinish()` 会把队列置为 finished，`PipelineExecutorContext::cancel()` 会 cancel 已注册的 shared queues。因此 empty/full/EOF/cancel 都有队列级状态兜底。

这与 `ExchangeReceiver` 的安全性边界类似：等待对象必须是一个能保存状态并在状态变化时唤醒 task 的队列，而不是一次性 detached thread 的单次 `notifyAll`。

**约束**：所有 producer source 对应的 `SharedQueueSinkOp` 都必须 exactly-once finish。最后一个 producer finish 后，`SharedQueue` 不会再接受新的 push。此时若 consumer 从 `EMPTY` 状态恢复，`SharedQueueSourceOp::readImpl()` 中 `tryPop` 返回 `FINISHED` 而非 `EMPTY`，consumer 直接收到 EOF，不会陷入 `WAIT_FOR_NOTIFY`。因此 EOF 场景也是安全的。

### Profile 归档

profile 记录时机需要调整。`addColumnarTableScanProfileInfos` 应在 pipeline group 1 的 `ColumnarReadSourceOp` 添加之后、`SharedQueueSinkOp` 添加之前记录，指向真正执行 columnar read/deserialize 的 source，而不是后续 queue sink 或 consumer source：

```cpp
auto [sink_holder, source_holder] = SharedQueue::build(exec_context, producer_num, source_num, -1, 4);

// Group 1: ColumnarReadSourceOp (真正的 IO 工作)
for (size_t i = 0; i < producer_num; ++i)
    group_builder.addConcurrency(std::make_unique<ColumnarReadSourceOp>(exec_context, log->identifier(), task_pool));

// 记录 profile 到 ColumnarReadSourceOp。必须在 setSinkOp(SharedQueueSinkOp) 前执行，
// 否则 getCurIOProfileInfos() 会看到 sink profile。
addColumnarTableScanProfileInfos(context, group_builder, table_scan);

group_builder.transform([&](auto & builder) {
    builder.setSinkOp(std::make_unique<SharedQueueSinkOp>(exec_context, log->identifier(), sink_holder));
});

// Group 2: queue consumer
auto header = group_builder.getCurrentHeader();
group_builder.addGroup();
for (size_t i = 0; i < source_num; ++i)
    group_builder.addConcurrency(
        std::make_unique<SharedQueueSourceOp>(exec_context, log->identifier(), header, source_holder));
```

`ColumnarReadSourceOp::getIOProfileInfo()` 应沿用当前 `RNColumnarSourceOp` 的 table scan IO profile 语义。`SharedQueueSinkOp` / `SharedQueueSourceOp` 的 profile 不应被归档为 table scan profile。

### 与现有 transform 链的顺序

两个 group 形成如下 pipeline 结构：

```text
Group 1:  ColumnarReadSourceOp → SharedQueueSinkOp
                                      │
                              SharedQueue  (bounded)
                                      │
Group 2:  SharedQueueSourceOp / ColumnarQueueSourceOp
            → GeneratedColumnPlaceHolder → extraCast
            → filterConditionsWithPushedDownFilters → projection
```

`executeGeneratedColumnPlaceholder`、`extraCast`、`filterConditionsWithPushedDownFilters` 等 transform 依然在 group 2 的 queue source 之后添加，无需改动。

### 与 reader materialize 异步机制的集成

`ColumnarReadSourceOp` 复用当前 `RNColumnarSourceOp` 的 inline materialize 逻辑，而不是恢复 `startAsyncMaterializeReader` + `WAIT_FOR_NOTIFY`：

```cpp
// ColumnarReadSourceOp::awaitImpl / executeIOImpl:
// - acquire reader work
// - Ready -> consume reader
// - NotStarted/Creating -> IO_IN -> createColumnarReaderWithBackoff inline
// - Failed/Consumed -> throw
// - reader Ready -> create input stream -> READING
```

如果后续希望重新引入 detached prefetch，需要先补一个带完成态的 one-shot future/latch，或者禁用同一个 work 的并发 materialize。否则 prefetch thread 和 producer source 都可能对同一个 `RNColumnarReaderWork` 调 `createColumnarReaderWithBackoff()`，造成重复 FFI reader 创建和 loser reader 释放问题。

### 与 stream 路径的兼容

stream 路径（`StorageDisaggregated::readThroughColumnar(const Context&, unsigned)`）完全不受影响，`RNColumnarInputStream` 保持不变。

## Incremental Modification Plan

### Phase 6: 新增 ColumnarReadSourceOp + SharedQueue 连接

修改文件：
* 新增 `dbms/src/Storages/Columnar/ColumnarReadSourceOp.h`
* 新增 `dbms/src/Storages/Columnar/ColumnarReadSourceOp.cpp`
* 修改 `dbms/src/Storages/Columnar/ColumnarSourceOp.h` — 如保留 `RNColumnarSourceOp` 名字，则将其简化为 queue consumer；也可以直接改用 `SharedQueueSourceOp`
* 修改 `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp` — 移除 reader work 管理、executeIOImpl / awaitImpl，仅保留 queue pop 逻辑或删除该类
* 修改 `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — `readThroughColumnar` pipeline overload 改为双 group + SharedQueue 连接
* 修改 `dbms/CMakeLists.txt` — 添加新文件

实现顺序：
1. 先把当前 `RNColumnarSourceOp` 中 reader work 管理、inline materialize、`RNColumnarInputStream::createWithReader` 相关逻辑搬到 `ColumnarReadSourceOp`，保持每次 `executeIOImpl()` 最多读一个 block。
2. producer group 添加 `producer_num` 个 `ColumnarReadSourceOp`。
3. 在 producer source 添加后、`SharedQueueSinkOp` 添加前记录 table scan profile。
4. 给 producer group 挂 `SharedQueueSinkOp`，再 `addGroup()` 创建 consumer group。
5. consumer group 使用 `SharedQueueSourceOp` 或轻量 `ColumnarQueueSourceOp`，随后接 generated column、extra cast、filter、projection。

### Phase 7: Profile 调整 + 测试

修改文件：
* `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — profile 记录调整
* 扩展 `dbms/src/Storages/tests/gtest_storage_disaggregated_columnar.cpp`
* 端到端验证

## Validation Strategy

### Unit Tests

* `ColumnarReadSourceOp` 状态转换：有 block、reader creating、reader failed、EOF、cancelled
* `SharedQueue` 连接：producer push → consumer pop → 结果一致
* 反压行为：queue full 时 producer 返回 WAIT_FOR_NOTIFY；queue empty 时 consumer 返回 WAIT_FOR_NOTIFY
* Profile 归档：table scan profile 指向 `ColumnarReadSourceOp`，不会被 `SharedQueueSinkOp` 或 consumer source 覆盖
* EOF 传播：producer finish → consumer 收到 EOF
* reader materialize：`NotStarted` / `Creating` 不返回 reader-work `WAIT_FOR_NOTIFY`，而是进入 `IO_IN`

### Integration Tests

* disaggregated compute + `use_columnar=1` + pipeline executor 的 table scan
* 带 generated column / timestamp cast / filter 的 table scan
* 高并发查询下 IO pool 占用正常（不垄断 IO 线程）
* `EXPLAIN ANALYZE` 中 table scan runtime stats 正确
* `tiflash_pipeline_wait_on_notify_tasks` metrics 有 `type_wait_on_shared_queue_read` / `type_wait_on_shared_queue_write`

### Runtime Checks

* IO pool 和 CPU pool 的利用率更均衡（不再出现 IO pool 繁忙而 CPU pool 空闲的反比）
* `EXPLAIN ANALYZE` 中 table scan 的 IO wait time 不包括 transform 时间

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| ColumnarReadSourceOp 崩溃导致 SharedQueue 永久空 | 下游 consumer 永久 WAIT_FOR_NOTIFY | 依赖 `PipelineExecutorContext::cancel()` cancel registered shared queues；若 producer 在本地捕获异常，也必须触发 query cancel |
| 队列长度选择不当（太小→频繁切换，太大→内存压力） | 性能不如串行模型 | 默认 cap=4，通过配置项可调；A/B 测试确定最优值 |
| profile 在挂 SharedQueueSinkOp 后归档 | EXPLAIN ANALYZE 统计到 queue sink，而不是 table scan IO | 必须在 `setSinkOp(SharedQueueSinkOp)` 前记录 `ColumnarReadSourceOp` 的 profile |
| 多 producer 读同一个 ColumnarReaderPtr | 查询结果错误 | 共享 work queue 只分配 reader work；每个 `ColumnarReadSourceOp` 独占自己的 current reader |
| detached prefetch 与 inline materialize 同时创建同一个 work | Rust ptr 泄露、重复创建 reader、状态覆盖 | producer model 下禁用 pipeline prefetch，或增加 owner/claim 状态和 loser reader 显式释放 |
| SharedQueue 带来额外的 block 拷贝开销 | 吞吐略降 | `LooseBoundedMPMCQueue` 使用 `std::move` 语义，block 是 move 的，无额外深拷贝 |
| SharedQueue 的 WAIT_FOR_NOTIFY 可能产生 lost wakeup | task 永久等待 | 使用已有 `SharedQueue` holder 的 read/write waiter、finish、cancel 语义；不要自定义一次性 notify 队列 |

## Alternatives Considered

### 方案 B: 不拆分 group，在 executeIOImpl 中批量读

保持单 group，让 `executeIOImpl()` 一次读多个 block 并缓存。问题：读多个 block 会在 IO pool 中停留时间更长，影响公平性，且仍无法实现 IO/CPU 并行。

### 方案 C: 用 detached thread 做生产者

用自定义线程从 columnar reader 读取 block 并写入一个自定义队列。**已否决**：基于 [lost wakeup race 实测结论](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race)，detached thread + 自定义队列的 `WAIT_FOR_NOTIFY` 模式在 producer 为一次性执行时会产生不可恢复的 missed wakeup。此外 detached thread 绕过 TaskScheduler，无法享受统一的公平性、取消和指标。

**选择当前方案 A 的理由**：复用已有 `SharedQueue` + `PipelineExecGroupBuilder::addGroup()` 基础设施，改动较小，符合当前 `SourceOp -> SinkOp` 的 pipeline builder contract，且 IO/CPU 并行收益明确。

## Open Questions

1. **SharedQueue 最优 capacity 是多少？** 需要 A/B 测试确定。初步建议 cap=4，过大可能导致过多 block 在内存中堆积。

2. **producer_num 默认值是多少？** 建议默认 `min(source_num, reader_count)`，但需要用 workload A/B 比较 `1`、`source_num` 和较小上限，避免 IO pool 过度占用。

3. **是否完全禁用 pipeline prefetch？** 为避免同一个 work 被 prefetch 和 producer inline 同时 materialize，建议 producer model 初版禁用 detached prefetch；如果保留，需要先设计 owner/claim 状态。

4. **是否保留 `RNColumnarSourceOp` 类名？** 如果 consumer 只是 queue pop，直接复用 `SharedQueueSourceOp` 最简单；如果需要保留 operator name/profile 兼容，可以引入 `ColumnarQueueSourceOp`，但不要再把 reader work 管理放回 consumer source。
