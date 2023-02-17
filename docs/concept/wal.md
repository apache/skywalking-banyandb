# Background

Write Ahead Logging (WAL) is a technique used in databases to ensure that data is not lost due to system crashes or other failures. The basic idea of WAL is to log changes to a database in a separate file before applying them to the database itself. This way, if there is a system failure, the database can be recovered by replaying the log of changes from the WAL file.
BanyanDB leverages the WAL to enhance the data buffer for schema resource writing. In such a system, write operations are first written to the WAL file before being applied to the interval buffer. This ensures that the log is written to disk before the actual data is written. Hence the term "write ahead".

# Format

![](https://skywalking.apache.org/doc-graph/banyandb/v0.4.0/wal-format.png)

A segment refers to a block of data in the WAL file that contains a sequence of database changes. Once `rotate` is invoked, a new segment is created to continue logging subsequent changes.
A "WALEntry" is a data unit representing a series of changes to a Series. Each WALEntry is written to a segment.

WAlEntry contains as follows:
- Length:8 bytes, which means the length of a WalEntry.
- Series ID:8 bytes, the same as request Series ID.
- Count:4 bytes, how many binary/timestamps in one WalEntry.
- Timestamp:8 bytes.
- Binary Length:2 bytes.
- Binary: value in the write request.

# Write process

![](https://skywalking.apache.org/doc-graph/banyandb/v0.4.0/wal.png)

The writing process in WAL is as follows:

1. Firstly, the changes are first written to the write buffer. Those with the same series ID will go to the identical WALEntry.
2. When the buffer is full, the WALEntry is created, then flushed to the disk. WAL can optionally use the compression algorithm snappy to compress the data on disk. Each WALEntry is appended to the tail of the WAL file on the disk.

When entries in the buffer are flushed to the disk, the callback function returned by the write operation is invoked. You can ignore this function to improve the writing performance, but it risks losing data.
# Read WAL
A client could read a single segment by a segment id. When opening the segment file, the reader will decompress the WAL file if the writing compresses the data.

# Rotation
WAL supports rotation operation to switch to a new segment. The operation closes the currently open segment and opens a new one, returning the closed segment details.

# Delete
A client could delete a segment closed by the `rotate` operation.

# configuration
BanyanDB WAL has the following configuration options:

| Name | Default Value | Introduction |
| --- | --- | --- |
| wal_compression | true | Compress the WAL entry or not |
| wal_file_size | 64MB | The size of the WAL file|
| wal_buffer_size | 16kB | The size of WAL buffer. |
