# Series Metadata 验证脚本（Standalone 模式）

### 1. 运行 banyandb

```bash
banyand/build/bin/dev/banyand-server standalone --observability-modes=native
```

### 2. 运行写入和验证脚本

```bash
go run scripts/write_measure_data.go \
  -grpc-addr=localhost:17912 \
  -group=test_series_metadata \
  -measure=test_measure \
  -data-path=/tmp/measure \
  -mode=standalone
```

**参数说明**：
- `-grpc-addr`: BanyanDB gRPC 服务地址（默认: localhost:17912）
- `-group`: Group 名称（默认: test_group）
- `-measure`: Measure 名称（默认: test_measure）
- `-data-path`: BanyanDB 数据路径（默认: /tmp/measure）
- `-mode`: 运行模式，`standalone` 或 `cluster`（默认: standalone）

### 3. 脚本会自动完成

1. 连接到 BanyanDB gRPC 服务
2. 创建 group（如果不存在）
3. 创建非 IndexMode 的 measure（如果不存在）
4. 写入 10 个数据点
5. 等待数据 flush（6 秒）
6. 验证 `series-metadata.bin` 文件是否存在


## 预期输出

成功时：
```
2025/12/10 11:52:19 ✓ Group 'test_series_metadata' created or already exists
2025/12/10 11:52:19 ✓ Measure 'test_measure' created or already exists
2025/12/10 11:52:19 Waiting for group 'test_series_metadata' to be loaded (3 seconds)...
2025/12/10 11:52:22 ✓ Data points written successfully
2025/12/10 11:52:22 Waiting for group to be loaded and data to be flushed (8 seconds)...
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:22 Warning: Write response status: STATUS_SUCCEED
2025/12/10 11:52:30 Checking for series-metadata.bin in: /tmp/measure (group: test_series_metadata)
2025/12/10 11:52:30 Checking latest part: /tmp/measure/data/test_series_metadata/seg-2025121011/shard-0/0000000000000001
2025/12/10 11:52:30 Looking for: /tmp/measure/data/test_series_metadata/seg-2025121011/shard-0/0000000000000001/series-metadata.bin
2025/12/10 11:52:30 ✓ Found series-metadata.bin
2025/12/10 11:52:30   Path: /tmp/measure/data/test_series_metadata/seg-2025121011/shard-0/0000000000000001/series-metadata.bin
2025/12/10 11:52:30   Size: 447 bytes
2025/12/10 11:52:30 
All files in part directory:
2025/12/10 11:52:30   - default.tf (200 bytes)
2025/12/10 11:52:30   - default.tfm (93 bytes)
2025/12/10 11:52:30   - fv.bin (200 bytes)
2025/12/10 11:52:30   - meta.bin (49 bytes)
2025/12/10 11:52:30   - metadata.json (158 bytes)
2025/12/10 11:52:30   - primary.bin (320 bytes)
2025/12/10 11:52:30   - series-metadata.bin (447 bytes)
2025/12/10 11:52:30   - timestamps.bin (0 bytes)
2025/12/10 11:52:30 ✓ Verification completed successfully!
```

## 注意

1. **只有非 IndexMode 的 measure 才会生成 series-metadata.bin**
   - IndexMode 的 measure 数据只存在索引中，不写入 part 文件夹
   - 脚本创建的 measure 默认是 `index_mode = false`

2. **文件只在本地生成**
   - 在 standalone 模式：文件在 `/tmp/measure` 下

