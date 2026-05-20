# 编译

## tiflash 编译

```bash
# 代码位置
# /DATA/disk1/jaysonhuang/tiflash/

# 编译(已经带 -DENABLE_NEXT_GEN=1 -DENABLE_NEXT_GEN_COLUMNAR=1)
cd /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng
# 加载工具链
source /DATA/disk1/ra_common/.tiflash_env_17_basic
# 使用 ninja 编译
ninja -j 16 tiflash gtests_dbms

# 编译后的 binary 位置
# /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng/dbms/src/Server/tiflash
```

## tikv/tikv-worker 编译
```bash
cd /DATA/disk1/jaysonhuang/cloud-storage-engine/
make build

# 编译后的 binary 位置
# /DATA/disk1/jaysonhuang/cloud-storage-engine/target/debug/tikv-server
# /DATA/disk1/jaysonhuang/cloud-storage-engine/target/debug/tikv-worker
```

# 启动

前提：PD（`:6530`）和 TiDB（`:8031`）已运行。存储组件按 **TiKV → tikv-worker → TiFlash** 顺序启动。

## 启动 TiKV

```bash
/DATA/disk1/jaysonhuang/cloud-storage-engine/target/debug/tikv-server \
    --addr "0.0.0.0:7530" \
    --advertise-addr "10.2.12.81:7530" \
    --status-addr "0.0.0.0:16530" \
    --advertise-status-addr "10.2.12.81:16530" \
    --pd "10.2.12.81:6530" \
    --data-dir "/DATA/disk3/jaysonhuang/clusters/tikv-7530/data" \
    --config /DATA/disk3/jaysonhuang/clusters/tikv-7530/conf/tikv.toml \
    --log-file "/DATA/disk3/jaysonhuang/clusters/tikv-7530/log/tikv.log" \
    >> /DATA/disk3/jaysonhuang/clusters/tikv-7530/log/tikv.stdout 2>&1 &

# tikv 日志目录
/DATA/disk3/jaysonhuang/clusters/tikv-7530/log
```

## 启动 tikv-worker

```bash
/DATA/disk1/jaysonhuang/cloud-storage-engine/target/debug/tikv-worker \
    --addr "0.0.0.0:19030" \
    --pd-endpoints "10.2.12.81:6530" \
    --data-dir "/DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/data" \
    --config /DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/tikv-worker.toml \
    --log-file "/DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/log/tikv.log" \
    >> /DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/log/tikv.stdout 2>&1 &

# tikv-worker 日志目录
/DATA/disk3/jaysonhuang/clusters/tikv-worker-19030/log
```

## 启动 tiflash-compute（tiflash-proxy-columnar）

必须使用 `server` 子命令，并在 `Server` 目录下设置 `LD_LIBRARY_PATH`（`libtiflash_proxy.so`、`libc++.so` 在同目录）。

```bash
cd /DATA/disk1/jaysonhuang/tiflash/cmake-build-debug-ng/dbms/src/Server
export LD_LIBRARY_PATH=$PWD:$LD_LIBRARY_PATH
./tiflash server --config-file /DATA/disk3/jaysonhuang/clusters/tiflash-5035/conf/tiflash.toml \
    >> /DATA/disk3/jaysonhuang/clusters/tiflash-5035/log/tiflash.stdout 2>&1 &

# tiflash 日志目录
/DATA/disk3/jaysonhuang/clusters/tiflash-5035/log
```

## tidb 连接方式

```bash
mycli -h 10.2.12.81 --port 8031 -D test -u root
```

# 待找出根因的测试

清理表请只用 SQL（`drop table`），不要手动删除 `data/` 目录下的文件。

```SQL
mysql> drop table if exists test.t_enum;
mysql> create table test.t_enum (pk enum('tidb','pd','tikv','tiflash') primary key clustered);
mysql> insert into test.t_enum values('tidb'),('tiflash');
mysql> alter table test.t_enum set tiflash replica 1;
-- 等待 replica AVAILABLE=1（约 30s，可查 information_schema.tiflash_replica）

# 预期的正确输出
mysql> set tidb_isolation_read_engines=tiflash;select * from test.t_enum;
+---------+
| pk      |
+---------+
| tidb    |
| tiflash |
+---------+

# 已知错误现象（enum PK + columnar）
mysql> set tidb_isolation_read_engines=tiflash; select pk, pk+0 from test.t_enum order by pk+0;
-- 错误：空 pk/0.0, tidb/1.0（缺 tiflash/4.0）

mysql> drop table if exists test.t_enum;
```
