# StorageDisaggregated Columnar Read 接入 Pipeline 执行模型设计

* Author(s): Codex
* Date: 2026-06-09
* Related doc: [2023-06-07-tiflash-pipeline-model.md](./2023-06-07-tiflash-pipeline-model.md)

## Summary

本文说明两件事：

1. 当前 TiFlash 中 `ExchangeReceiver` 是如何接入 pipeline executor 的。
2. 如果要把 `StorageDisaggregated::readThroughColumnar` 路径完整接入 pipeline 执行模型，应如何组织 source operator、异步等待、IO 执行、profile 统计和验证。

当前代码中已经存在 `StorageDisaggregated::readThroughColumnar(PipelineExecutorContext &, PipelineExecGroupBuilder &, ...)` 和 `RNProxySourceOp`，说明 columnar 路径已经有基础 pipeline 接入。但这个接入主要把 proxy 读取放到 pipeline IO 线程池中，reader materialize 阶段仍可能通过 `std::condition_variable` 阻塞 IO 线程，且 table scan 的 inbound IO profile / source profile 归档不如 TiFlash-write disaggregated 路径完整。本文建议保留当前入口形态，补齐等待通知、profile、空读路径和取消语义，使其更符合 pipeline model 的调度边界。

## Context

Pipeline model 将物理执行计划按 pipeline breaker 切成 pipeline DAG，再把 pipeline DAG 转成 event DAG。每个 event 生成一个或多个 `PipelineTask`，由 `TaskScheduler` 在 CPU task thread pool、IO task thread pool 和 Wait Reactor 之间切换。

关键状态含义如下：

| OperatorStatus | Task 去向 | 典型含义 |
| --- | --- | --- |
| `HAS_OUTPUT` / `NEED_INPUT` | CPU task thread pool 继续执行 | operator 可继续推进 |
| `IO_IN` / `IO_OUT` | IO task thread pool | 需要执行读写、反序列化或其他 IO 密集工作 |
| `WAITING` | Wait Reactor 轮询 `await()` | 条件可能很快变为 ready，但没有外部通知对象 |
| `WAIT_FOR_NOTIFY` | 注册到 `NotifyFuture` | 等待队列、网络、reader 等对象主动唤醒 |
| `FINISHED` / `CANCELLED` | task 结束 | pipeline task 生命周期结束 |

`PipelineExec` 负责把 `SourceOp -> TransformOp -> SinkOp` 串起来。它在执行过程中遇到 `IO_*` 会暂存 `io_op`，遇到 `WAITING` 会暂存 `awaitable`，遇到 `WAIT_FOR_NOTIFY` 会暂存 `waiting_for_notify`。`PipelineTaskBase` 再把这些 operator 状态映射为 task 状态，交给 scheduler 调度。

当前 columnar pipeline 路径的调度边界还不够清晰。`RNProxySourceOp::readImpl()` 在 CPU task thread pool 中做轻量状态判断和输出暂存 block，generated column placeholder、timestamp/duration cast、filter 以及 table scan projection 等 transform 也在 CPU 执行路径继续推进。`RNProxySourceOp::awaitImpl()` 当前直接返回 `IO_IN`，因此 source 会进入 IO task thread pool；随后 `executeIOImpl()` 创建 `RNProxyInputStream`，调用 proxy FFI 读取 block、逐列读取数据、反序列化列，并把结果暂存在 `t_block`。同时，`RNProxyReadTask::prefetchReader()` 会通过 `ThreadManager::scheduleThenDetach` 启动 `PrefetchRNProxyReader` 线程异步 materialize 下一个 `ColumnarReader`。

这种实现可以避免在 CPU task thread pool 中直接执行 proxy read，但会带来以下性能问题：

* IO worker 可能被等待型工作阻塞。若 `getOrCreateReader()` 发现 reader 正在 `Creating`，当前实现会在 IO 线程里通过 `std::condition_variable::wait()` 等待；如果 reader materialize 遇到 region miss、lock resolve、PD/backoff 或 proxy 慢调用，IO worker 可能长期占用却没有实际读数据。
* Pipeline scheduler 的让出和公平性会变差。scheduler 只有在 operator 返回 `OperatorStatus` 后才能重新调度；长时间的 `fn_get_columnar_reader`、`fn_read_block`、backoff 或 blocking wait 会让 task 无法及时 yield，影响同 query 其他 source 和其他 query 的 IO task 尾延迟。
* 观测会失真。reader materialize wait、proxy read、列反序列化都可能被计入 source operator 的 IO/执行时间，而不是 `WAIT_FOR_NOTIFY` 或明确的等待时间，导致 `EXPLAIN ANALYZE` 和 pipeline scheduler metrics 难以区分真 IO 慢、reader 创建慢、region/lock/backoff 慢还是 prefetch 等待。
* 额外 prefetch 线程绕过 pipeline scheduler。`PrefetchRNProxyReader` 不在 CPU/IO task queue 中排队，不受 pipeline task scheduler 的统一公平性和资源控制；高并发下它会与 IO pool 同时竞争 CPU、proxy FFI、region cache、PD/lock resolver 等资源，并可能提前持有 reader/Rust 侧资源。
* IO pool 中混入较重 CPU 工作。`RNProxyInputStream::readImpl()` 除了调用 proxy FFI，还会逐列反序列化 ClickHouse column 并填充 extra table id；如果反序列化成本高，IO pool 会被 CPU-heavy 工作占用，CPU pool 则等待 source 输出，造成 CPU/IO pool 负载不均。
* 查询启动阶段仍有非 scheduler 工作。`readThroughColumnar()` 在 pipeline source ops 建立前构建 `RNProxyReadTask` 和 reader plans；如果该阶段涉及 region cache miss、bucket key 查询或 backoff，会直接增加 query startup latency，且不受 pipeline task 调度约束。

因此，本文后续设计重点不是简单地把更多逻辑放进 IO task thread pool，而是把“真正需要执行 IO/FFI/反序列化的工作”和“等待 reader 或数据 ready 的工作”分开：前者继续返回 `IO_IN`，后者应尽量返回 `WAIT_FOR_NOTIFY` 并由 producer 主动唤醒。

## ExchangeReceiver 如何接入 Pipeline

`ExchangeReceiver` 的 pipeline 接入可以分成四层。

### 1. MPPTask 先创建 ExchangeReceiver

`MPPTask::initExchangeReceivers()` 遍历 DAGRequest，遇到 `tipb::ExecType::TypeExchangeReceiver` 时创建 `ExchangeReceiver`，并放入 `MPPReceiverSet`。之后 `DAGContext::getMPPExchangeReceiver(executor_id)` 会从这个 receiver set 中取出对应 receiver。

因此 pipeline 构建阶段不会再解析 gRPC task meta，也不会重新建连接；它只拿到已经初始化好的 `ExchangeReceiver`。

### 2. PhysicalExchangeReceiver 构建 pipeline source

`PhysicalExchangeReceiver::build()` 从 `DAGContext` 取 `ExchangeReceiver`，用 receiver 的 output schema 生成 physical node schema。

`PhysicalExchangeReceiver::buildPipelineExecGroupImpl()` 按并发度生成多个 `ExchangeReceiverSourceOp`：

```text
PhysicalExchangeReceiver
  -> PipelineExecGroupBuilder::addConcurrency(ExchangeReceiverSourceOp)
  -> DAGContext::addInboundIOProfileInfos(executor_id, source_io_profiles)
```

如果启用 fine grained shuffle，并发度会被限制到 `fine_grained_shuffle.stream_count`，每个 source 使用不同 `stream_id`；否则多个 source 都从 stream `0` 读取。

### 3. ExchangeReceiverSourceOp 非阻塞读取队列

`ExchangeReceiverSourceOp::readImpl()` 不调用阻塞的 `receive()`，而是调用 `exchange_receiver->tryReceive(stream_id, recv_msg)`。

读取结果的处理方式是：

* 队列有数据：调用 `toExchangeReceiveResult()` 解码 packet/chunk，填充本地 `block_queue`，返回 `HAS_OUTPUT`。
* 队列 EOF：flush decoder，返回一个空 block 的 `HAS_OUTPUT`，让下游 operator 按 source EOF 语义结束。
* 队列为空：返回 `WAIT_FOR_NOTIFY`。

队列为空时，`ReceivedMessageQueue::pop<false>()` 会把对应的 gRPC queue 或 fine-grained message channel 设置到 thread-local `current_notify_future`。这一步是关键：operator 自己只返回状态，真正的 task 注册发生在 task scheduler 层。

### 4. NotifyFuture 唤醒 task

当 task 从 CPU/IO 线程池返回 `WAIT_FOR_NOTIFY` 时，scheduler 调用 `registerTaskToFuture(std::move(task))`，把 task 注册到 `current_notify_future`。

当网络层收到 packet 并写入队列时，队列的 `NotifyFuture` 通过 `PipeConditionVariable` 唤醒等待 task：

```text
packet arrives
  -> queue notify
  -> task.notify()
  -> PipelineExec::notify()
  -> ExchangeReceiverSourceOp::notify()
  -> TaskScheduler::submit(task)
```

`ExchangeReceiverSourceOp::notifyImpl()` 当前不需要做额外事情，`PipelineExec::notify()` 的主要作用是清掉 `waiting_for_notify`，让下一次 `execute()` 从正常读路径继续。

这个设计让 ExchangeReceiver 满足 pipeline model 的核心要求：没有数据时不占 CPU，不阻塞 IO worker；有数据时由生产者主动唤醒对应 task。

## StorageDisaggregated Columnar 当前路径

`StorageDisaggregated` 在 disaggregated compute mode 下根据 `isReadColumnar()` 分两类路径：

* `readThroughTiFlashWrite`：从 TiFlash-write node 建 remote segment read tasks。
* `readThroughColumnar`：通过 columnar proxy 创建 `ColumnarReader` 并读取列式数据。

当前 columnar pipeline 路径的主要流程是：

```text
StorageDisaggregated::read(...)
  -> readThroughColumnar(exec_context, group_builder, ...)
  -> RNProxyReadTask::buildProxyReadTaskWithBackoff(...)
  -> group_builder.addConcurrency(RNProxySourceOp)
  -> GeneratedColumnPlaceHolderTransformOp
  -> extraCast
  -> filterConditionsWithPushedDownFilters
  -> PhysicalTableScan::buildProjection
```

`RNProxySourceOp` 的当前行为：

* `readImpl()` 如果有暂存 block 则输出，否则返回 `IO_IN`。
* `executeIOImpl()` 领取一个 reader index，创建 `RNProxyInputStream`，调用 proxy FFI `fn_read_block()`，反序列化列数据并暂存 block。
* `prefetchReader()` 会用 detached thread 预创建下一个 `ColumnarReader`。
* 如果 `getOrCreateReader()` 发现 reader 正在 `Creating`，会用 `std::condition_variable` 等待。

这已经避免了在 CPU task thread pool 中直接执行 proxy read，但还没有完全复用 `ExchangeReceiver` 那种 `NotifyFuture` 等待模型。

## Goals

* Columnar read source 必须是 pipeline source operator，而不是 `BlockInputStreamSourceOp` 包装旧流。
* 读取、反序列化、reader materialize 等 IO/FFI 工作应通过 `IO_IN` 进入 IO task thread pool。
* 等待异步 reader materialize 或队列数据时，应优先使用 `WAIT_FOR_NOTIFY`，避免阻塞 IO worker 或让 Wait Reactor 忙轮询。
* 保持 stream model 和 pipeline model 的结果语义一致，包括 generated column placeholder、duration/timestamp cast、late-materialization filter 的二次过滤、partition table `_tidb_tid`。
* 正确记录 table scan 的 source operator profile 和 inbound IO profile，避免把后续 projection/filter 的 profile 错记为 table scan。
* 支持取消、异常、Rust FFI 指针释放和空 range / 空 reader 情况。

## Non-Goals

* 不改变 columnar proxy 的 FFI 协议。
* 不改变 TiDB 下发的 DAGRequest、table scan schema 或 filter pushdown 语义。
* 不在本设计中实现新的 reader 切分算法；沿用当前按 region / bucket split 生成 `RNProxyReaderPlan` 的方式。
* 不要求一次性替换 stream model。旧的 `RNProxyInputStream` 路径仍保留，用于非 pipeline 执行和对照验证。

## Proposed Design

### 1. 保留 StorageDisaggregated 的双重 read 入口

`StorageDisaggregated::read(...)` 的 pipeline overload 应继续按当前结构分派：

```text
if (isReadColumnar())
    readThroughColumnar(exec_context, group_builder, context, num_streams)
else
    readThroughTiFlashWrite(exec_context, group_builder, context, num_streams)
```

`PhysicalTableScan::buildPipeline()` 已经通过 `StorageDisaggregatedInterpreter::execute(exec_context, pipeline_exec_builder)` 进入这个 overload，因此不需要在 planner 中新增特殊 case。

### 2. 将 RNProxySourceOp 作为唯一 pipeline source

Columnar pipeline 路径应使用 `RNProxySourceOp` 直接产出 blocks：

```text
RNProxyReadTask(shared reader plans)
  -> RNProxySourceOp #0
  -> RNProxySourceOp #1
  -> ...
```

每个 source 从共享 `RNProxyReadTask` 原子领取 reader index。这样 reader 数量可以大于 pipeline 并发度，source 读完一个 reader 后继续领取下一个，直到所有 reader 消费完毕。

建议保留当前 `RNProxyReadTask` 的共享池形态，不为每个 reader 创建单独 pipeline task。原因是 reader plan 数量可能随 region/bucket split 变化，直接映射成 task 会放大 task 数量；source 内部领取 reader 能保持 pipeline 并发度稳定。

### 3. 拆分 source 的四类状态

`RNProxySourceOp` 应明确维护四类状态：

```text
READY_BLOCK: t_block 有 block
READING:     current_input_stream 正在被 IO 线程读取
WAIT_READER: 已领取 reader index，但 reader materialize 尚未完成
DONE:        所有 reader 已消费完
```

建议状态转移：

```text
readImpl()
  if DONE: emit empty block, HAS_OUTPUT
  if READY_BLOCK: emit block, HAS_OUTPUT
  if current reader can read: IO_IN
  if waiting async reader: WAIT_FOR_NOTIFY
  else acquire next reader

executeIOImpl()
  materialize reader if this source owns creation
  read one block through proxy FFI
  deserialize columns and fill extra table id
  cache block in t_block
  return HAS_OUTPUT

awaitImpl()
  check reader slot state without blocking
  Ready/Failed/Consumed -> HAS_OUTPUT or throw
  Creating -> WAIT_FOR_NOTIFY
  NotStarted -> IO_IN or start async materialize then WAIT_FOR_NOTIFY
```

关键约束：`awaitImpl()` 不应使用 `std::condition_variable::wait()` 阻塞。它只做非阻塞状态检查。

### 4. 用 NotifyFuture 替代阻塞等待 reader materialize

当前 `RNProxyReaderSlot` 使用 `std::condition_variable`。为了贴合 pipeline model，建议扩展它，使其拥有一个 `PipeConditionVariable` 并实现通知注册能力：

```cpp
struct RNProxyReaderSlot : public NotifyFuture
{
    void registerTask(TaskPtr && task) override
    {
        task->setNotifyType(NotifyType::WAIT_ON_TABLE_SCAN_READ);
        pipe_cv.registerTask(std::move(task));
    }

    void notifyAll()
    {
        pipe_cv.notifyAll();
    }

    std::mutex mutex;
    PipeConditionVariable pipe_cv;
    RNProxyReaderMaterializeState state;
    std::optional<ColumnarReaderPtr> reader;
    std::exception_ptr exception;
};
```

当 source 发现 slot 正在 `Creating` 时：

```cpp
setNotifyFuture(slot.get());
return OperatorStatus::WAIT_FOR_NOTIFY;
```

当 prefetch thread 创建 reader 成功或失败时：

```cpp
slot->state = Ready 或 Failed;
slot->notifyAll();
```

这样等待 reader materialize 的 task 会像 ExchangeReceiver 一样被生产者唤醒，而不是阻塞 IO 线程。

如果不想让 `RNProxyReaderSlot` 直接继承 `NotifyFuture`，也可以给每个 slot 持有一个小的 `RNProxyReaderNotifyFuture` 对象；核心要求是 source 可以把 `current_notify_future` 指向一个能唤醒 task 的对象。

### 5. Reader materialize 和 block read 的执行边界

`fn_get_columnar_reader` 和 `fn_read_block` 都可能包含 FFI、存储访问、反序列化或远端等待，应避免在 CPU task thread pool 中执行。

建议规则：

* source 第一次领取 reader 后，如果 slot 是 `NotStarted`，可以返回 `IO_IN`，由 `executeIOImpl()` 同步 materialize 当前 reader。
* source 读当前 reader 时，`fn_read_block` 和列反序列化放在 `executeIOImpl()` 中。
* source 读当前 reader 的同时，可异步 prefetch 后一个 reader。等待 prefetch 结果时用 `WAIT_FOR_NOTIFY`。
* 每次 `executeIOImpl()` 最多产出一个 block，然后返回 `HAS_OUTPUT` 交回 CPU 侧继续 pipeline，避免一个 IO task 长时间独占 worker。

### 6. Profile 归档

Columnar pipeline 路径应像 `buildRemoteSegmentSourceOps()` 一样，在 source ops 加入 builder 后立即记录 table scan 的 source profile 和 inbound IO profile：

```cpp
auto table_scan_id = table_scan.getTableScanExecutorID();
context.getDAGContext()->addInboundIOProfileInfos(
    table_scan_id,
    group_builder.getCurIOProfileInfos());
context.getDAGContext()->addOperatorProfileInfos(
    table_scan_id,
    group_builder.getCurProfileInfos());
```

这一步必须发生在 generated column placeholder、extra cast、filter 和 table scan projection 之前。否则 `PhysicalPlanNode::buildPipelineExecGroup()` 会在最后记录当前 builder 的 profile，可能把 projection 或 filter profile 记成 table scan。

`RNProxySourceOp::getIOProfileInfo()` 当前返回 local IO profile。是否要归类为 remote 取决于 proxy 是否能提供连接类型和远端字节统计：

* 如果 proxy 只能提供本地 FFI 读出的 block bytes，保持 `IOProfileInfo::createForLocal(profile_info_ptr)`。
* 如果 proxy 后续能返回 region/store 级连接信息，则改为 `createForRemote` 并填充 `connection_profile_infos`。

### 7. Filter、generated column 和 cast 顺序

Columnar 路径必须保持当前顺序：

```text
RNProxySourceOp
  -> GeneratedColumnPlaceHolderTransformOp
  -> extraCast(include_pushed_down_filter_columns = true)
  -> filterConditionsWithPushedDownFilters
  -> table scan schema projection
```

原因：

* proxy reader 暂不读取 generated columns，必须在本地补 placeholder。
* timestamp / duration cast 仍需在 TiFlash 侧执行；对 proxy late-materialization filter 涉及的列也要 cast。
* proxy 的 late-materialization filter 只保证减少加载 pack，不保证过滤掉所有不满足条件的行，因此必须在 TiFlash pipeline 中重新应用 pushed-down filters。
* 最后由 `PhysicalTableScan::buildProjection()` 保证输出列名和 TiDB table scan schema 一致。

### 8. 空 reader / 空 range 处理

如果 `buildProxyReadTaskWithBackoff()` 返回空任务，或任务中 reader count 为 0，pipeline builder 不能保持为空。应添加 `NullSourceOp`：

```cpp
Block header(getColumnWithTypeAndName(genNamesAndTypesForTableScan(table_scan)));
group_builder.addConcurrency(std::make_unique<NullSourceOp>(exec_context, header, log->identifier()));
```

并记录 table scan profile。这样后续 `group_builder.getCurrentHeader()`、generated column、cast 和 projection 都有合法输入 header，行为也与 `DAGStorageInterpreter` 的空读路径一致。

### 9. 取消和异常传播

需要补齐 `RNProxyReadTask` / reader slot 的取消语义：

* `RNProxySourceOp::operateSuffixImpl()` 或析构时，如果 source 未正常读完，应标记共享 task cancelled。
* `RNProxyReadTask::cancel(reason)` 应把所有 `NotStarted/Creating` slot 置为 Failed 或 Cancelled，并 `notifyAll()`。
* prefetch thread 捕获异常后写入 slot exception 并通知等待 task。
* source 在 `awaitImpl()` 或 `executeIOImpl()` 看到 Failed/Cancelled 时抛出异常，由 `Task::execute()` 捕获并触发 `PipelineExecutorContext::onErrorOccurred()`。
* Rust FFI 指针仍由 slot / input stream 的析构和 scope guard 释放，确保 error path 不泄露。

## Incremental Modification Plan

### Phase 1: 拆分 pipeline source 相关代码

当前 `StorageDisaggregatedColumnar.cpp` 同时包含 read planning、FFI reader 创建、stream path 和 pipeline source op。后续 notify、状态机、取消语义都会集中修改 `RNProxySourceOp` / reader slot，如果继续放在同一个大文件中，review 很难区分“机械搬运”和“行为变化”。因此第一阶段先做代码拆分，尽量不改变行为。

修改文件：

* 新增 `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
* 新增 `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
* `dbms/src/Storages/StorageDisaggregatedColumnar.h`
* `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`
* 对应 `CMakeLists.txt`

改动：

1. 将 `RNProxySourceOp`、source 状态成员、pipeline source helper 从 `StorageDisaggregatedColumnar.*` 拆到新文件。
2. 将与 source 强相关的 reader slot notify/状态访问接口一并整理到新头文件，或者至少为后续改造预留窄接口，避免 source 直接依赖 planning 细节。
3. 保留 `RNProxyInputStream` 和 stream path 在 `StorageDisaggregatedColumnar.*` 中，除非需要共享小型 helper；第一阶段不改变 stream 行为。
4. 拆分后先跑现有 columnar/disaggregated 相关编译目标或最小 gtest，确认这是纯结构性改动。

### Phase 2: 修正现有 pipeline columnar 路径的统计和空读

修改文件：

* `dbms/src/Storages/StorageDisaggregatedColumnar.cpp`
* 必要时包含 `Operators/NullSourceOp.h`

改动：

1. 在 `readThroughColumnar(exec_context, group_builder, ...)` 中处理 `read_proxy_tasks.empty()` 和 `getReaderCount() == 0`，构建 `NullSourceOp`。
2. 在添加 `RNProxySourceOp` 后，立即调用 `addInboundIOProfileInfos(table_scan_id, ...)` 和 `addOperatorProfileInfos(table_scan_id, ...)`。
3. 确保后续 generated column、extra cast、filter 不覆盖 table scan source profile。

### Phase 3: 将 reader materialize 等待改成 NotifyFuture

修改文件：

* `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
* `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
* 必要时调整 `dbms/src/Storages/StorageDisaggregatedColumnar.*` 中共享 reader task 接口
* 如需新增 metrics label，修改 pipeline notify metrics 相关文件；否则复用 `WAIT_ON_TABLE_SCAN_READ`。

改动：

1. 给 `RNProxyReaderSlot` 增加 `PipeConditionVariable` 或等价 `NotifyFuture` 包装。
2. 将 `getOrCreateReader()` 中对 `Creating` 的阻塞等待拆成非阻塞 `tryGetReadyReader()`。
3. `RNProxySourceOp::awaitImpl()` 在 slot 未 ready 时 `setNotifyFuture(slot)` 并返回 `WAIT_FOR_NOTIFY`。
4. `prefetchReader()` 结束时调用 slot notify。
5. 保留 stream path 的阻塞 `getOrCreateReader()`，或让 stream path 使用单独的 blocking helper，避免影响旧执行模型。

### Phase 4: 收敛 source 状态机

修改文件：

* `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
* `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`

改动：

1. 给 `RNProxySourceOp` 明确记录当前 reader slot、当前 reader、暂存 block 和 done 状态。
2. 保证 `readImpl()` 不做 proxy FFI 调用。
3. 保证 `awaitImpl()` 不阻塞、不分配大对象，只检查状态并返回 `HAS_OUTPUT` / `IO_IN` / `WAIT_FOR_NOTIFY`。
4. 保证 `executeIOImpl()` 每次最多读取一个 block。

### Phase 5: 取消、错误和资源释放

修改文件：

* `dbms/src/Storages/Columnar/ColumnarSourceOp.h`
* `dbms/src/Storages/Columnar/ColumnarSourceOp.cpp`
* 必要时调整 `dbms/src/Storages/StorageDisaggregatedColumnar.*` 中共享 reader task 生命周期接口

改动：

1. 增加 `RNProxyReadTask::cancel()`。
2. source suffix/destructor 对未完成共享 task 做 cancel 或 ref-counted close。
3. prefetch thread 在 cancel 后不再写入 reader，必要时释放 Rust ptr。
4. 所有 Failed/Cancelled slot 必须唤醒等待 task。

## Validation Strategy

### Unit Tests

建议新增或扩展 gtest：

* `RNProxySourceOp` 状态转换：有 block、EOF、reader creating、reader failed、cancelled。
* `RNProxyReaderSlot` notify：task 注册后，prefetch 完成能唤醒 task 并重新提交。
* 空 reader / 空 range：pipeline builder 生成 `NullSourceOp`，不触发 `getCurrentHeader()` 断言。
* profile 归档：`DAGContext::inbound_io_profile_infos_map[table_scan_id]` 存在，`operator_profile_infos_map[table_scan_id]` 指向 source profile 而不是 projection/filter。

### Integration / Interpreter Tests

建议覆盖：

* disaggregated compute + `use_columnar` + pipeline executor 的 table scan。
* 带 generated column 的 table scan。
* 带 timestamp / duration cast 的 table scan。
* 带 pushed-down filter / late-materialization filter 的 table scan，验证 TiFlash 侧二次过滤正确。
* partition table scan，验证 `_tidb_tid` / extra table id column。
* region miss / lock / PD error 重试路径。

### Runtime Checks

* `PipelineExecutor::toString()` 能看到 table scan 所在 pipeline。
* `EXPLAIN ANALYZE` 中 table scan runtime stats 不丢失。
* `tiflash_pipeline_task_change_to_status` 中 `IO_IN`、`WAIT_FOR_NOTIFY` 状态变化符合预期。
* 高并发查询下 IO worker 不因 reader prefetch wait 长时间阻塞。

## Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| reader materialize 改成 notify 后出现 missed wakeup | task 永久等待 | slot 状态修改和 task 注册都必须在同一 mutex / condition protocol 下完成；注册后再次检查状态 |
| profile 归档时机不对 | EXPLAIN ANALYZE 表达错误 | 在 source 加入 builder 后立即记录 table scan source profile，并保持 `addOperatorProfileInfos` 的 first-write-wins 语义 |
| cancel 与 prefetch thread 竞争 | Rust ptr 泄露或 double free | slot 拥有 reader ptr，状态切换后只允许一个消费者 move reader；未返回 reader 用 scope guard / slot 析构释放 |
| `WAIT_FOR_NOTIFY` metrics 类型不精确 | 观测上无法区分 proxy reader 和普通 table scan | 第一阶段复用 `WAIT_ON_TABLE_SCAN_READ`；如需要更细粒度，再新增 notify type 和 metrics label |
| stream path 与 pipeline path 行为漂移 | 两种执行模式结果不一致 | 保留 `RNProxyInputStream` 对照测试，重点比较 block schema、rows、filters、casts 和 error path |

## Compatibility

* `ENABLE_NEXT_GEN_COLUMNAR == 0` 时仍走 `StorageDisaggregatedRemote.cpp` 中的 placeholder，行为不变。
* 非 disaggregated compute mode 不受影响。
* TiFlash-write disaggregated 路径不需要修改。
* Pipeline 和 stream 两套执行模型可以并存；修改应只增强 pipeline overload。

## Conclusion

`ExchangeReceiver` 接入 pipeline 的核心不是把旧 stream 包起来，而是把网络接收抽象成 `SourceOp`，通过 `tryReceive -> WAIT_FOR_NOTIFY -> NotifyFuture -> task.notify()` 与 scheduler 对接。

`StorageDisaggregated::readThroughColumnar` 应采用同样原则：`RNProxySourceOp` 作为 source，proxy 读取走 `IO_IN`，reader 未 ready 走 `WAIT_FOR_NOTIFY`，数据 ready 后主动唤醒 task。当前代码已经具备 source operator 和 IO pool 接入基础；后续重点是补齐 notify 型等待、table scan profile 归档、空读路径和取消语义。
