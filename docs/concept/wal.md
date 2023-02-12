# Wal Format

![](/assets/wal-format.jpg)

Wal file consist of a series of WalEntry，WalEntry contain as follows:

- Length:8 bytes,means length of a WalEntry.
- Series ID:8 bytes,the same as request Series ID.
- Count:4 bytes,how many binary/timestamp in one WalEntry. 
- Timestamp:8 bytes.
- Binary Length:2 bytes.
- Binary:value in write request.

# Write process

![](/assets/wal.jpg)

When client send a request,the the request will be sent to the tsdb instance corresponding to the group.And when tsdb receives a write request, the following steps occur:

1. The write request is store in wal buffer .Request  in buffer will aggregate a WalEntry .For the performance reasons,we use compression algorithm snappy.
2. WalEntry appended to the end of the WAL file in disk,It is batch write.
3. The data buffer is updated.
4. The write request is successful and return.

BanyanDB support wal configuration，you can omit step 2 if there is a requirement for performance and data loss can be tolerated.Pay attention to it will cause losing data possibly.
Even if you can close wal when node only used to data forwarding.

# Rotation
Because of the strict write order，The request record in wal file is the same as data buffer.So when data buffer flush in disk,wal file will delete and a new will create,and delete is logical delete,not delete really. 

# API
`rotate()`

This rotate API used to delete data which had been flush in disk.The following scenarios may use this API

- Flush data buffer to disk.
- Replay wal file when BanyanDB instance init. 

`read()`

This API used for read wal file.Corresponding to the following scenarios:

- Replay wal file when init BanyanDB instance init.
- Wal replicate.

# configuration
BanyanDB support configuration parameter：

| Name | Default Value | Introduction |
| --- | --- | --- |
| wal | open | Open defaultly, close when value is close. |
| wal_persistent | open | Open defaultly, you can set close if there is a requirement for performance and data loss can be tolerated. |
| wal_compression | open | Compression defalutly,you can close it by using close value |
| wal_file_size | 64MB | The size of wal file.We recommand set just to store data in data buffer. |
| wal_buffer_size | 16kB | The size of wal buffer. |

