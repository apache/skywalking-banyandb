# Series Metadata 集群模式验证

## 启动 etcd

```bash
etcd --name etcd1 \
  --data-dir /tmp/etcd1 \
  --listen-client-urls http://localhost:2379 \
  --advertise-client-urls http://localhost:2379
```

## 启动 Data（可选，用于验证同步机制）

**注意：为了验证 series-metadata.bin 文件，建议先不启动 Data 节点，或者先启动后停止**

```bash
banyand/build/bin/dev/banyand-server data \
  --etcd-endpoints=http://localhost:2379 \
  --measure-root-path=/tmp/data-node1/measure \
  --stream-root-path=/tmp/data-node1/stream \
  --grpc-port=17920 \
  --http-port=17921 \
  --logging-levels=debug
```

## 启动 Liaison 

```bash
banyand/build/bin/dev/banyand-server liaison \
  --etcd-endpoints=http://localhost:2379 \
  --measure-root-path=/tmp/liaison-node/measure \
  --stream-root-path=/tmp/liaison-node/stream \
  --grpc-port=17912 \
  --http-port=17913 \
  --observability-modes=native \
  --logging-levels=debug
```

**参数说明**：
- `-grpc-addr`: BanyanDB gRPC 服务地址（默认: localhost:17912）
- `-group`: Group 名称（默认: test_group）
- `-measure`: Measure 名称（默认: test_measure）
- `-data-path`: BanyanDB 数据路径（默认: /tmp/measure）
- `-mode`: 运行模式，`standalone` 或 `cluster`（默认: standalone）
  - **集群模式必须指定 `-mode=cluster`**，这样脚本会在正确的路径查找文件

**重要提示**：
- `-data-path` 参数应该指向 **Liaison 节点** 的数据目录，而不是 Data 节点的目录
- 在集群模式下，`series-metadata.bin` 文件存储在 `{data-path}/measure/data/{group}/shard-{id}/{part_id}/` 路径下（没有 seg-* 目录）
- **关键**：正常情况下，liaison 中的 part 文件会很快同步到 data 节点，同步成功后整个 part 目录（包括 series-metadata.bin）会被删除，所以 liaison 目录通常是空的。要验证文件存在，需要停止 data 节点让同步失败，这样 part 会在 liaison 中积压。

## 验证结果

### 5.1 验证 Liaison 节点上的文件

### 5.2 验证 Data 节点上**没有**该文件

手动检查 Data 节点的数据目录

## 注意

1. **只有非 IndexMode 的 measure 才会生成 series-metadata.bin**
   - IndexMode 的 measure 数据只存在索引中，不写入 part 文件夹
   - 脚本创建的 measure 默认是 `index_mode = false`

2. **文件只在 Liaison 节点生成（Cluster 模式）**
   - `series-metadata.bin` 文件只在 Cluster 模式的 Liaison 节点的数据目录下生成
   - **不会同步到 Data 节点**（代码中明确排除）
   - **但同步成功后，整个 part 目录会被删除**，包括 series-metadata.bin
   - **Standalone 模式不会生成此文件**

3. **文件路径**
   - Cluster 模式（Liaison）：`{data-path}/measure/data/{group}/shard-{id}/{part_id}/`（没有 seg-* 目录）
   - 注意：`-data-path` 参数应该指向 **Liaison 节点** 的数据目录