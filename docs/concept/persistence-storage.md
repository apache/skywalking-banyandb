# Persistence Storage
Persistence storage is used for unifying data of BanyanDB persistence， including write-ahead logging(WAL)， index, and data collected from skywalking and other observability platforms or APM systems. It provides various implementations and IO modes to satisfy the need of different components.
BanyanDB provides a concise interface that shields the complexity of the implementation from the upper layer. By exposing necessary interfaces, making upper components do not need to care how persistence implement and mask differences between different operating systems.

# IO Mode
persistence storage support different IO mode to satisfy different throughput requirements. The interface is not directly exposed to developers and can be set through configuration.

## Io_uring
Io_uring is a new feature in Linux 5.1, which provides high throughput and implements in a fully asynchronous way. In the scene of massive storage, io_uring can bring significant benefits.

## Synchronous IO
Synchronous IO is the most common IO mode, but the throughput is relatively low. BanyanDB provides a nonblocking mode that can be used in lower Linux versions.

# Operation
## Directory
### Create
Create the specified directory and return an error if the directory already exists.

### Delete
Delete the directory and all files and return an error if the directory does not exist or the directory not reading or writing.

### rename
Rename the directory and return an error if the directory already exists.

### Get Children
Get all lists of files or children's directories in the directory and return an error if the directory does not exist.

## File
### Write
BanyanDB provides two methods for writing files.
Append mode, which adds new data to the end of a file. This mode is typically used for WAL.
Flush mode, which flushes all data to one or more files and can specify the size of each file to write to.
It will return an error when writing a directory, the file does not exist or there is not enough space.

### Delete
BanyanDB provides a batch-deleting operation, which can delete several files at once. it will return an error if the directory does not exist or the file not reading or writing.

### Read
For reading operation, two read methods are provided:
Reading a specified location of data, which relies on a specified offset and a buffer.
Read the entire file, BanyanDB provides stream reading, which can use when the file is too large, the size gets each time can be set when using stream reading.
If entering incorrect parameters such as incorrect offset or non-existent file, it will return an error.

### rename
Rename the file and return an error if the directory exists in this directory.

### Get size
Get the file size and return an error if the file does not exist.