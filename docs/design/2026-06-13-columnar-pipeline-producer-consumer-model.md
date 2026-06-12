# StorageDisaggregated Columnar: 生产者-消费者流水线读取模型

* Author(s): JaySon-Huang
* Date: 2026-06-13
* Related docs:
  * [2026-06-09-storage-disaggregated-columnar-pipeline.md](./2026-06-09-storage-disaggregated-columnar-pipeline.md)
  * [2026-06-09-storage-disaggregated-columnar-pipeline-impl.md](./2026-06-09-storage-disaggregated-columnar-pipeline-impl.md)

## Summary

当前 `RNColumnarSourceOp` 的 pipeline 模型是"串行乒乓"：每次调用 `executeIOImpl()` 读取并反序列化一个 block，返回 `HAS_OUTPUT` 后走完整的 CPU transform 链（generated column placeholder → cast → filter → projection），然后再回到 IO pool 读下一个 block。同一个 pipeline task 内 IO 和 CPU 严格串行，无法利用流水线重叠。

本文提议将 columnar 读取路径改为 **生产者-消费者流水线模型**：新增一个 `ColumnarReadOp`（SinkOp）作为 IO 生产者，持续读取 block 并推入有界 `SharedQueue`；`RNColumnarSourceOp` 改为从 `SharedQueue` pop block（类似 `ExchangeReceiverSourceOp` 从 block_queue 读取）。两个 pipeline group 通过 `SharedQueue` 连接，实现 IO 读取与 CPU transform 的并行执行。

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

* 将 `fn_read_block` + 列反序列化从 `RNColumnarSourceOp::executeIOImpl()` 中分离出来，放入独立的 `ColumnarReadOp`（SinkOp），在 IO pool 中持续执行
* `RNColumnarSourceOp` 改为从有界 `SharedQueue` pop block，只在 CPU pool 中执行轻量状态检查
* 通过 `SharedQueue` 的长度上限实现反压：IO 过快时 `ColumnarReadOp` 返回 `WAIT_FOR_NOTIFY`，释放 IO 线程；CPU 消费不足时 `RNColumnarSourceOp` 返回 `WAIT_FOR_NOTIFY`
* 保持与现有 generated column placeholder → extra cast → filter → projection 的顺序兼容性
* 保持与 stream 路径（`RNColumnarInputStream`）的行为一致，不改变 stream 路径
* 正确归档 table scan 的 source profile 和 inbound IO profile

## Non-Goals

* 不改变 columnar helper 的 FFI 协议
* 不改变 reader materialize 的异步机制（`startAsyncMaterializeReader` + `WAIT_FOR_NOTIFY` 保持不变）
* 不改变 reader work 的分配策略（`tryAcquireReaderWork` + `prefetchPendingWork` 保持不变）
* 不改变 TiDB 下发的 DAGRequest、table scan schema 或 filter pushdown 语义
* 不增加 `ColumnarReadOp` 的并发度 — columnar reader 非线程安全，producer 并发度固定为 1
* 不为非 pipeline 路径实现此模型

## Design

### 架构概览

```text
readThroughColumnar (pipeline overload)
  │
  ├─ [Group 1] IO Producer Group (concurrency = 1)
  │     ColumnarReadOp (SinkOp)
  │       ├─ awaitImpl() → 等待 reader materialize (复用现有异步机制)
  │       ├─ executeIOImpl() → fn_read_block + deserialize → push to SharedQueue
  │       └─ writeImpl() → tryPush to SharedQueue → full? WAIT_FOR_NOTIFY
  │
  ├─ SharedQueue (bounded, cap = 2~4 blocks)
  │     ┌───┬───┬───┬───┐
  │     │   │   │   │   │
  │     └───┴───┴───┴───┘
  │       ↑ push        ↓ pop
  │
  ├─ [Group 2] CPU Consumer Group (concurrency = source_num)
  │     RNColumnarSourceOp (SourceOp)
  │       → readImpl() → tryPop from SharedQueue → empty? WAIT_FOR_NOTIFY
  │       → 有 block → HAS_OUTPUT → 下游 transform 链
  │
  ├─ executeGeneratedColumnPlaceholder
  ├─ extraCast
  ├─ filterConditionsWithPushedDownFilters
  └─ addColumnarTableScanProfileInfos
```

### 关键组件

#### ColumnarReadOp (新增 SinkOp)

```cpp
class ColumnarReadOp : public SinkOp
{
public:
    ColumnarReadOp(
        PipelineExecutorContext & exec_context,
        const String & req_id,
        const SharedQueueSinkHolderPtr & shared_queue_holder,
        RNColumnarReadTaskPtr task);

protected:
    OperatorStatus prepareImpl() override;   // 等待 reader materialize
    OperatorStatus awaitImpl() override;      // reader Creating → WAIT_FOR_NOTIFY
    OperatorStatus executeIOImpl() override;  // fn_read_block + deserialize
    OperatorStatus writeImpl(Block && block) override; // tryPush to SharedQueue

private:
    SharedQueueSinkHolderPtr shared_queue_holder;
    RNColumnarReadTaskPtr task;
    ColumnarReaderPtr current_reader;
    BlockInputStreamPtr current_input_stream;
    // ... reader work management, reuse existing async materialize pattern
};
```

**职责**：
- 管理 reader work 的生命周期（acquire → wait for materialize → read）
- 在 `executeIOImpl()` 中调用 `fn_read_block` + 列反序列化，产出一个 block
- 在 `writeImpl()` 中 `tryPush` 到 `SharedQueue`：
  - 成功 → `NEED_INPUT`（继续读下一个 block）
  - 队列满 → `setNotifyFuture` + `WAIT_FOR_NOTIFY`
- reader 读完所有 block → reset → acquire next reader work → 循环

**并发度**：固定为 1。原因有两个：
1. columnar reader 不是线程安全的（持有 Rust FFI pointer）
2. 一个查询的表扫描通常只需要一个 producer（reader work 本身就是串行消费的）

#### RNColumnarSourceOp (修改)

简化为轻量级 SharedQueue consumer：

```cpp
OperatorStatus RNColumnarSourceOp::readImpl(Block & block)
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

**不再需要** `executeIOImpl()` 和 `awaitImpl()`（所有 IO/等待逻辑移到 `ColumnarReadOp`）。

#### SharedQueue 配置

```cpp
// cap = 2~4 blocks，折中 IO/CPU 流水线重叠与内存占用
SharedQueue::build(
    exec_context,
    /*producer=*/1,
    /*consumer=*/source_num,
    /*max_buffered_bytes=*/-1,  // 不限字节数，仅限 block 数
    /*max_queue_size=*/4
);
```

`SharedQueue` 基于 `LooseBoundedMPMCQueue<Block>`，`tryPush` 满时返回 `MPMCQueueResult::FULL`，`tryPop` 空时返回 `MPMCQueueResult::EMPTY`。

### 反压流程

```text
场景 A: IO 快于 CPU (queue full):
  ColumnarReadOp::writeImpl()
    → tryPush → FULL
    → setNotifyFuture(shared_queue_sink_holder)
    → WAIT_FOR_NOTIFY ──► 释放 IO 线程
  RNColumnarSourceOp 消费后 → shared_queue.notifyAll() → ColumnarReadOp 被唤醒 → 继续 push

场景 B: CPU 快于 IO (queue empty):
  RNColumnarSourceOp::readImpl()
    → tryPop → EMPTY
    → setNotifyFuture(shared_queue_source_holder)
    → WAIT_FOR_NOTIFY ──► 释放 CPU 线程
  ColumnarReadOp push 后 → shared_queue.notifyAll() → RNColumnarSourceOp 被唤醒 → 继续 pop

场景 C: producer EOF (所有 reader work 读完):
  ColumnarReadOp::~ColumnarReadOp() / finish → shared_queue.producerFinish()
  RNColumnarSourceOp::tryPop → FINISHED → 发空 block → 下游正常结束
```

### SharedQueue WAIT_FOR_NOTIFY 的安全性

本方案中 producer（`ColumnarReadOp`）和 consumer（`RNColumnarSourceOp`）都使用了 `WAIT_FOR_NOTIFY`，但其安全性条件与 columnar detached thread 的 `WAIT_FOR_NOTIFY` 有根本区别。

[设计文档 §Observed Issue](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race) 详细分析了 columnar reader materialize 中 `WAIT_FOR_NOTIFY` 的 lost wakeup race：因为 producer（detached Prefetch thread）是一次性的——每个 reader work 只 materialize 一次，`notifyAll` 只调用一次——如果这次调用时 waiter 尚未注册到 `pipe_cv`，wakeup 永久丢失。

`SharedQueue` 的 `WAIT_FOR_NOTIFY` 不会产生此 race，原因有两个：

1. **Producer 和 consumer 都是持久运行的 pipeline task**。`ColumnarReadOp` 作为一个 SinkOp，只要还有 reader work 可读，就会继续 push block。`RNColumnarSourceOp` 作为 SourceOp，只要下游还需要数据，就会继续 pop。它们不会像 detached thread 那样执行完一次 materialize 后消失。

2. **每次 push/pop 都可能触发新的 `notifyAll`**。`SharedQueue` 内基于 `LooseBoundedMPMCQueue`，每次 `tryPop`（释放一个 slot）或 `tryPush`（写入一个 block）后，另一方若在等待，会在后续 pop/push 时被唤醒。Wakeup 是**可重入的**——即使某次 `notifyAll` 时 waiter 尚未注册，下一次 push/pop 操作会再次触发通知。

这与 `ExchangeReceiver` 的安全性保证相同：gRPC 持续收包 → 持续 push → 持续 `notifyAll`。SharedQueue 的 IO 侧（ColumnarReadOp 持续读 block）和 CPU 侧（RNColumnarSourceOp 持续消费）构成同样的可重入模式。

**约束**：如果 producer（ColumnarReadOp）读完所有 reader work 后退出（调用 `producerFinish()`），此后 `SharedQueue` 不会再接受新的 push。此时若 consumer 尚未从 `EMPTY` 状态恢复，`SharedQueueSourceOp::readImpl()` 中 `tryPop` 返回 `FINISHED` 而非 `EMPTY`，consumer 直接收到 EOF，不会陷入 `WAIT_FOR_NOTIFY`。因此 EOF 场景也是安全的。

### Profile 归档

profile 记录时机需要调整。`addColumnarTableScanProfileInfos` 应在 pipeline group 1 的 source 之后记录，指向 `ColumnarReadOp` 而非 `RNColumnarSourceOp`：

```cpp
// Group 1: ColumnarReadOp (IO 工作)
auto [sink_holder, source_holder] = SharedQueue::build(exec_context, 1, source_num, -1, 4);
// ... add ColumnarReadOp as SinkOp for each builder in group 1 ...

// 记录 profile 到 ColumnarReadOp（真正的 IO 发生在这一层）
group_builder.getCurGroup().front().getCurIOProfileInfo()  // ColumnarReadOp's IO profile

// Group 2: RNColumnarSourceOp (轻量级 queue consumer)
group_builder.addGroup();
for (size_t i = 0; i < source_num; ++i)
    group_builder.addConcurrency(std::make_unique<RNColumnarSourceOp>(
        exec_context, log->identifier(), header, source_holder));
```

具体实现可能需要将 `ColumnarReadOp` 也实现 `getIOProfileInfo()`，或者通过 `SharedQueueSinkOp` 的 profile 间接记录。

### 与现有 transform 链的顺序

两个 group 形成如下 pipeline 结构：

```text
Group 1:  (none) → ColumnarReadOp (SinkOp)
                            │
                    SharedQueue  (bounded)
                            │
Group 2:  RNColumnarSourceOp → GeneratedColumnPlaceHolder → extraCast
                              → filterConditionsWithPushedDownFilters → projection
```

`executeGeneratedColumnPlaceholder`、`extraCast`、`filterConditionsWithPushedDownFilters` 等 transform 依然在 group 2 的 `RNColumnarSourceOp` 之后添加，无需改动。

### 与 reader materialize 异步机制的集成

不改变 `startAsyncMaterializeReader` + `WAIT_FOR_NOTIFY` 模式。`ColumnarReadOp` 内部复用相同的逻辑：

```cpp
// ColumnarReadOp::prepareImpl / awaitImpl:
// 与当前 RNColumnarSourceOp::awaitImpl 的 NEED_READER / WAIT_READER 逻辑相同
// - acquire reader work → tryGetReadyReader → 触发 async materialize → WAIT_FOR_NOTIFY
// - reader Ready → 创建 input stream → 进入 READING
```

### 与 stream 路径的兼容

stream 路径（`StorageDisaggregated::readThroughColumnar(const Context&, unsigned)`）完全不受影响，`RNColumnarInputStream` 保持不变。

## Incremental Modification Plan

### Phase 6: 新增 ColumnarReadOp + SharedQueue 连接

修改文件：
* 新增 `dbms/src/Storages/Columnar/ColumnarReadOp.h`
* 新增 `dbms/src/Storages/Columnar/ColumnarReadOp.cpp`
* 修改 `dbms/src/Storages/Columnar/ColumnarSourceOp.h` — 简化为 SharedQueue consumer
* 修改 `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp` — 移除 executeIOImpl / awaitImpl，仅保留 readImpl
* 修改 `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — `readThroughColumnar` pipeline overload 改为双 group + SharedQueue 连接
* 修改 `dbms/CMakeLists.txt` — 添加新文件

### Phase 7: Profile 调整 + 测试

修改文件：
* `dbms/src/Storages/StorageDisaggregatedColumnar.cpp` — profile 记录调整
* 扩展 `dbms/src/Storages/tests/gtest_storage_disaggregated_columnar.cpp`
* 端到端验证

## Validation Strategy

### Unit Tests

* `ColumnarReadOp` 状态转换：有 block、reader creating、reader failed、queue full、cancelled
* `SharedQueue` 连接：producer push → consumer pop → 结果一致
* 反压行为：queue full 时 producer 返回 WAIT_FOR_NOTIFY；queue empty 时 consumer 返回 WAIT_FOR_NOTIFY
* Profile 归档：`ColumnarReadOp` 的 IO profile 被错误地记为 `RNColumnarSourceOp` 的 profile
* EOF 传播：producer finish → consumer 收到 EOF

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
| ColumnarReadOp 崩溃导致 SharedQueue 永久空 | 下游 consumer 永久 WAIT_FOR_NOTIFY | `ColumnarReadOp` 析构时调 `queue->cancel()`，唤醒所有等待的 consumer task 并抛异常 |
| 队列长度选择不当（太小→频繁切换，太大→内存压力） | 性能不如串行模型 | 默认 cap=4，通过配置项可调；A/B 测试确定最优值 |
| profile 从 SourceOp 改为 SinkOp 后 EXPLAIN ANALYZE 统计丢失 | 观测盲区 | 确保 `DAGContext::addInboundIOProfileInfos` 记录到 `ColumnarReadOp` 的 IO profile |
| `ColumnarReadOp` 并发度 > 1 导致列数据错乱 | 查询结果错误 | 强制并发度为 1；添加 `RUNTIME_CHECK(source_num == 1)` 在 producer group |
| SharedQueue 带来额外的 block 拷贝开销 | 吞吐略降 | `LooseBoundedMPMCQueue` 使用 `std::move` 语义，block 是 move 的，无额外深拷贝 |
| SharedQueue 的 WAIT_FOR_NOTIFY 可能产生 lost wakeup | task 永久等待 | ✅ 已分析并排除。与 columnar detached thread 的单次 notifyAll 不同，SharedQueue 的 producer/consumer 都是持久 pipeline task，每次 push/pop 都可能触发新的 notifyAll，wakeup 可重入。详见[安全性分析](#sharedqueue-wait_for_notify-的安全性) |

## Alternatives Considered

### 方案 B: 不拆分 group，在 executeIOImpl 中批量读

保持单 group，让 `executeIOImpl()` 一次读多个 block 并缓存。问题：读多个 block 会在 IO pool 中停留时间更长，影响公平性，且仍无法实现 IO/CPU 并行。

### 方案 C: 用 detached thread 做生产者

用自定义线程从 columnar reader 读取 block 并写入一个自定义队列。**已否决**：基于 [lost wakeup race 实测结论](./2026-06-09-storage-disaggregated-columnar-pipeline.md#observed-issue-notstarted-async-materialize-lost-wakeup-race)，detached thread + 自定义队列的 `WAIT_FOR_NOTIFY` 模式在 producer 为一次性执行时会产生不可恢复的 missed wakeup。此外 detached thread 绕过 TaskScheduler，无法享受统一的公平性、取消和指标。

**选择当前方案 A 的理由**：复用已有 `SharedQueue` + `PipelineExecGroupBuilder::addGroup()` 基础设施，改动最小，符合 pipeline model 的调度边界，且 IO/CPU 并行收益明确。

## Open Questions

1. **SharedQueue 最优 capacity 是多少？** 需要 A/B 测试确定。初步建议 cap=4，过大可能导致过多 block 在内存中堆积。

2. **ColumnarReadOp 的 profile 如何在 DAGContext 中归档？** `ColumnarReadOp` 是 SinkOp，而 `addInboundIOProfileInfos` 通常记录 SourceOp 的 IO profile。需要确认 SinkOp 的 `getIOProfileInfo()` 能否被正确收集到 table scan 的 profile 中，或者需要在 `ColumnarReadOp` 中自定义 profile 记录逻辑。

3. **多个 `ColumnarReadOp` producer 是否可行？** 当前 columnar reader 非线程安全，不支持。未来如果 columnar helper 支持多个独立 reader 并行读取（例如按 region 拆分），可以考虑 producer 并发度 > 1。

4. **是否需要与现有的 `RNColumnarSourceOp` 类名区分？** 目前 `RNColumnarSourceOp` 承担了 reader work 管理和 block 读取双重职责。在 Phase 6 中，`RNColumnarSourceOp` 被简化为纯 SharedQueue consumer，而 `ColumnarReadOp` 接管 reader work 管理。是否需要将 `RNColumnarSourceOp` 重命名为类似 `ColumnarQueueSourceOp` 的名字，避免与历史代码混淆？
