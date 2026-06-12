# Columnar Pipeline 开发环境与测试指南

本文档描述 `StorageDisaggregated::readThroughColumnar` pipeline 路径相关的编译、单元测试和端到端测试流程。

## 环境准备

加载编译工具链环境：

```bash
source /data1/ra_common/.tiflash_env_17_basic
```

## 编译

### 完整编译

```bash
cd cmake-build-debug
source /data1/ra_common/.tiflash_env_17_basic
ninja tiflash
```

编译产物：
- 主程序：`cmake-build-debug/dbms/src/Server/tiflash`
- Proxy 库：`cmake-build-debug/contrib/tiflash-proxy-cmake/debug/libtiflash_proxy.so`
- 单元测试：`cmake-build-debug/dbms/gtests_dbms`

编译选项（已配置在 CMakeCache.txt 中）：

| 选项 | 用途 |
|---|---|
| `ENABLE_NEXT_GEN=ON` | 启用 disaggregated 架构支持 |
| `ENABLE_NEXT_GEN_COLUMNAR=ON` | 启用 columnar read 路径 |
| `ENABLE_TESTS=ON` | 启用测试 |

### 增量编译

修改代码后，在 `cmake-build-debug` 目录执行：

```bash
source /data1/ra_common/.tiflash_env_17_basic
ninja tiflash
```

CMake 的 `file(GLOB ...)` 机制通过 `cmake/dbms_glob_sources.cmake` 自动发现 `src/` 下的新 `.cpp/.h` 文件。新增文件后首次 `ninja` 会自动触发 re-glob 和重新配置。

## 单元测试

### 运行相关测试

Columnar/disaggregated 相关的单测均编译在 `gtests_dbms` 二进制中：

```bash
source /data1/ra_common/.tiflash_env_17_basic
cmake-build-debug/dbms/gtests_dbms --gtest_filter="StorageDisaggregated*"
```

测试文件位置：
- `dbms/src/Flash/tests/gtest_storage_disaggregated.cpp` — StorageDisaggregated 基础测试
- `dbms/src/Storages/tests/gtest_disagg_remote.cpp` — 远端 disaggregated helpers 测试

### 运行全部单测

```bash
source /data1/ra_common/.tiflash_env_17_basic
cmake-build-debug/dbms/gtests_dbms
```

### 并行加速

```bash
source /data1/ra_common/.tiflash_env_17_basic
python3 tests/gtest_10x.py cmake-build-debug/dbms/gtests_dbms
```

## 端到端测试 (Next-Gen Columnar)

### 测试目录结构

```
tests/fullstack-test-next-gen-columnar/
  ├── run.sh                    # 完整测试入口（up → test → down）
  ├── compose.sh -> ../docker/compose.sh  # docker compose 管理
  ├── _env.sh                   # 本地配置（binary 路径、镜像 tag）
  ├── next-gen-cluster.yaml -> ../docker/next-gen-columnar-yaml/cluster.yaml
  ├── disagg_tiflash.yaml -> ../docker/next-gen-columnar-yaml/disagg_tiflash.yaml
  ├── disagg_tiflash.rocky9.yaml  # Rocky Linux 9 specific override
  ├── next-gen-columnar-config/   # TiFlash/TiKV 配置文件
  └── data/ / log/                # 运行时数据和日志
```

### 集群组件

| 容器 | 角色 | 端口 |
|---|---|---|
| `pd0` | PD (Placement Driver) | 2379 |
| `tikv0` | TiKV storage node | 20160 |
| `tikv-worker0` | TiKV worker (columnar engine) | — |
| `tidb0` | TiDB SQL layer | 4000 |
| `tiflash-cn0` | TiFlash compute node (columnar) | 3930 (gRPC) |
| `minio0` | S3-compatible object storage | 9000 |

**关键配置**：`NEXT_GEN_COLUMNAR_ONLY=true`（由 `_env.sh` 设置），只启动 columnar 路径（不启动 tiflash-wn write node）。

### 集群生命周期管理

所有操作在 `tests/fullstack-test-next-gen-columnar/` 目录下执行。

**查看状态：**
```bash
cd tests/fullstack-test-next-gen-columnar
source /data1/ra_common/.tiflash_env_17_basic
source _env.sh
./compose.sh ps
```

**启动集群：**
```bash
./compose.sh up -d
```

**停止集群：**
```bash
./compose.sh down
```

**重启单个组件（如更换 binary 后）：**
```bash
./compose.sh stop tiflash-cn0
./compose.sh start tiflash-cn0
```

### 安装本地编译的 TiFlash binary

集群通过 `tests/docker/override-yaml/local_tiflash_columnar.yaml` 将宿主机的 `tests/.build/tiflash/` 目录挂载到容器内。因此需要将编译产物复制到该目录。

**首次或更新 binary：**

```bash
# 1. 先确保容器已停止（binary 被占用时无法覆盖）
cd tests/fullstack-test-next-gen-columnar
source /data1/ra_common/.tiflash_env_17_basic
source _env.sh
./compose.sh stop tiflash-cn0

# 2. 复制编译产物
cp cmake-build-debug/dbms/src/Server/tiflash tests/.build/tiflash/tiflash
cp cmake-build-debug/contrib/tiflash-proxy-cmake/debug/libtiflash_proxy.so \
   tests/.build/tiflash/libtiflash_proxy.so

# 3. 重新启动
./compose.sh start tiflash-cn0
```

> **注意**：直接 `cp` 到运行中的容器所挂载的 binary 会报 `Text file busy` 错误，必须先 stop 容器。

`tests/.build/tiflash/` 目录所需文件：
- `tiflash` — 主程序
- `libtiflash_proxy.so` — Rust FFI proxy 库
- `libc++.so.1`, `libc++abi.so.1` — LLVM C++ 运行时
- `libgmssl.so.3` — 国密 SSL 库

**也可通过 `_env.sh` 指定其他安装路径：**
```bash
export LOCAL_TiFLASH_BIN_DIR="/path/to/your/install/dir"
```

### 确认 TiFlash 已就绪

```bash
# 检查 gRPC 端口是否在监听
./compose.sh exec -T tiflash-cn0 bash -c 'tail -20 /log/tiflash.log'
```

看到 `"Flash grpc server listening on [0.0.0.0:3930]"` 表示服务已启动。

也可通过 TiDB 查询确认 TiFlash 节点状态：
```bash
./compose.sh exec -T tidb0 bash -c \
  "mysql -h 127.0.0.1 -P 4000 -u root -e 'select * from information_schema.tiflash_replica'"
```

### 运行单个端到端测试

```bash
cd tests/fullstack-test-next-gen-columnar
source /data1/ra_common/.tiflash_env_17_basic
source _env.sh

# 运行 sample 测试（最基础的表创建/写入/读取）
./compose.sh exec -T tiflash-cn0 bash -c \
  'cd /tests && ENABLE_NEXT_GEN=true verbose=true ./run-test.sh fullstack-test/sample.test'

# 运行 MPP 相关测试
./compose.sh exec -T tiflash-cn0 bash -c \
  'cd /tests && ENABLE_NEXT_GEN=true verbose=true ./run-test.sh fullstack-test2/mpp'

# 运行表达式测试
./compose.sh exec -T tiflash-cn0 bash -c \
  'cd /tests && ENABLE_NEXT_GEN=true verbose=true ./run-test.sh fullstack-test/expr'
```

### 运行完整测试套件

`run.sh` 会依次执行以下测试目录：

```
fullstack-test/sample.test       # 基础 CRUD
fullstack-test2/clustered_index  # 聚簇索引
fullstack-test2/dml              # DML 操作
fullstack-test2/variables        # 变量/配置
fullstack-test2/mpp              # MPP 执行
fullstack-test/expr              # 表达式
fullstack-test/mpp               # MPP 高级场景
```

```bash
cd tests/fullstack-test-next-gen-columnar
source /data1/ra_common/.tiflash_env_17_basic
ENABLE_NEXT_GEN=true ./run.sh
```

### 查看日志

```bash
# TiFlash compute node 日志
./compose.sh logs tiflash-cn0 | tail -50

# 错误日志
./compose.sh exec -T tiflash-cn0 bash -c 'tail -50 /log/tiflash_error.log'

# TiDB 日志
./compose.sh logs tidb0 | tail -30
```

### 手动进入容器调试

```bash
./compose.sh exec tiflash-cn0 bash
```

容器内可用资源：
- `/tiflash` — TiFlash binary
- `/tiflash.toml` — TiFlash 配置
- `/log/tiflash.log` — 运行日志
- `/log/tiflash_error.log` — 错误日志
- `/tests/` — 测试脚本和用例

### 常见问题

**Q: `Text file busy` 无法覆盖 binary**
→ 先 `./compose.sh stop tiflash-cn0`，再 cp，最后 `./compose.sh start tiflash-cn0`

**Q: 测试报 "Can not find exchange receiver"**
→ TiFlash 节点可能未就绪，等待 10-20 秒后重试，或检查 tiflash-cn0 日志

**Q: 编译报错找不到新文件**
→ 删除 `cmake-build-debug/CMakeCache.txt` 重新配置，或执行 `ninja rebuild_cache`

**Q: 单元测试报 "error while loading shared libraries"**
→ 确保已 `source /data1/ra_common/.tiflash_env_17_basic`

## 添加新的 Columnar 源文件

若在 `dbms/src/Storages/Columnar/` 下新增 `.h/.cpp` 文件，需在 `dbms/CMakeLists.txt` 中的 `src/Storages` 行（约第 88 行）附近添加：

```cmake
add_headers_and_sources(dbms src/Storages/Columnar)
```

通过 `cmake/dbms_glob_sources.cmake` 中的 `add_headers_and_sources` 宏，该行会自动 glob 该目录下的所有 `*.h` 和 `*.cpp` 文件。

## 相关文档

- [StorageDisaggregated Columnar Pipeline 设计文档](../design/2026-06-09-storage-disaggregated-columnar-pipeline.md)
- [StorageDisaggregated Columnar Pipeline 实现计划](../design/2026-06-09-storage-disaggregated-columnar-pipeline-impl.md)
- [TiFlash Pipeline Model 设计文档](../design/2023-06-07-tiflash-pipeline-model.md)
- [TiFlash Agent Guide](../../AGENTS.md)
- [测试指南](../../tests/AGENTS.md)
