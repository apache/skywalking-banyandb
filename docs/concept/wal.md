# Background

The data is stored in memory first, then will be flushed to disk. During this time, if service downtime, the data in memory will lose.
So the goal of the write-ahead logging(WAL) is to solve this problem. And WAL is designed as a dependent component.

# Format

![](/assets/wal-format.jpg)

Wal file consists of a series of segments, which have their WAL buffer, and data in the WAL buffer flush to every segment by appending WAlEntry.

WAlEntry contains as follows:
- Length:8 bytes, which means the length of a WalEntry.
- Series ID:8 bytes, the same as request Series ID.
- Count:4 bytes, how many binary/timestamps in one WalEntry.
- Timestamp:8 bytes.
- Binary Length:2 bytes.
- Binary: value in the write request.

# Write process

![](/assets/wal.jpg)

The writing process in WAL is as follows:

1. Store in the WAL buffer, Requests with the same series ID will aggregate a WALEntry.
2. When the buffer is full, the WALEntry will be flushed to the disk by batch write. For performance reasons, we use the compression algorithm snappy default. And WALEntry is appended to the end of the WAL file in the disk.

When WAL is flushed to the disk, a callback will generate. You can choose to ignore this callback function to obtain higher performance, but it will cause the risk of losing data.

# Read WAL
When reading the WAL file, it needs to decompress first. You can get the size of a WALEntry by the length in the header of every WALEntry so that you can read all the WALEntries.

# Rotation
WAL supports rotation operation to switch among segments.

# Delete
When the WAL file is invalid, it can be deleted.

# configuration
BanyanDB support configuration parameter：

| Name | Default Value | Introduction |
| --- | --- | --- |
| wal_compression | true | Compression default, you can close it by using false value |
| wal_file_size | 64MB | The size of the WAL file|
| wal_buffer_size | 16kB | The size of WAL buffer. |