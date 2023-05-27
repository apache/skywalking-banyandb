# Persistence Storage
Persistence storage is used for unifying data of BanyanDB persistence， including write-ahead logging(WAL)， index, and data collected from skywalking and other observability platforms or APM systems. By exposing necessary interfaces, making upper components do not need to care about how persistence implement. And BanyanDB provides concise interfaces that shield the complexity of the implementation from the upper layer to make it as easy to use as possible. BanyanDB provides various implementations of persistence and IO modes to satisfy the need of different components.

# IO Mode
persistence storage support different IO modes to satisfy different throughput requirements. The interface is not directly exposed to developers and can be set through configuration.

## Io_uring
Io_uring is a new feature in Linux 5.1, which provides high throughput and implements in a fully asynchronous way. In the sense of massive storage, io_uring can bring significant benefits.

## Synchronous IO
Synchronous IO is the most common IO mode, but the throughput is relatively low. BanyanDB provides a nonblocking mode that can be used in lower Linux versions.

# Write
BanyanDB designs different storage ways for different senses. There are two kinds of storage, one is append-only with no compaction, which can be chosen when writing WAL. The other is Log-Structured Merge Tree storage, data written on disk will become a sorted string table(SST) and will compaction in a specific size.
To guarantee idempotence, BanyanDB introduces multi-version concurrency control(MVCC). When data is recorded on the disk, the version will increase. Of course, not all data requires MVCC, for WAL, only appending to the end of the file.

# Update
When executing the update operation, the original data will not be truly overwritten. The updated value will be read first when read, so it will not affect the accuracy of the data.

# Delete
The same as the update operation, the original data will not be truly deleted when you execute the delete operation. The deleted value will be read before the old operation, so it also will not affect the accuracy of the data.

# Read
For the read operation, two read methods are provided, one is to read a specific piece of data, and the other is to read a certain range of data. For each of the read operations, you can choose to use version number or not.
BanyanDB guarantees that the data read is idempotent by using version, or returning the latest data if you don't use it.BanyanDB uses the index and filter to accelerate reading.

# Snapshot
BanyanDB allows building snapshots, which record the state of persistence storage at a specific time and can be used for backup and reading.