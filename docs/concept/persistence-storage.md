# Persistence Storage
Persistence storage is used for unifying data of BanyanDB persistence, including write-ahead logging(WAL), index, and data collected from skywalking and other observability platforms or APM systems. It provides various implementations and IO modes to satisfy the need of different components.
BanyanDB provides a concise interface that shields the complexity of the implementation from the upper layer. By exposing necessary interfaces, upper components do not need to care how persistence is implemented and avoid dealing with differences between different operating systems.

# IO Mode
Persistence storage offers a range of IO modes to cater to various throughput requirements. The interface can be accessed by developers and can be configured through settings.

## Io_uring
Io_uring is a new feature in Linux 5.1, which is fully asynchronous and offers high throughput. In the scene of massive storage, io_uring can bring significant benefits.

## Synchronous IO
The most common IO mode is Synchronous IO, but it has a relatively low throughput. BanyanDB provides a nonblocking mode that is compatible with lower Linux versions.

# Operation
## Directory
### Create
Create the specified directory and return an error if the directory already exists.

### Delete
Delete the directory and all files and return an error if the directory does not exist or the directory not reading or writing.

### Rename
Rename the directory and return an error if the directory already exists.

### Get Children
Get all lists of files or children's directories in the directory and return an error if the directory does not exist.

### Permission
When creating a file, the default owner is the user who created the directory. The owner can specify read and write permissions of the directory. If not specified, the default is read and write permissions, which include permissions for all files in the directory.

## File
### Write
BanyanDB provides two methods for writing files.
Append mode, which adds new data to the end of a file. This mode is typically used for WAL.
Flush mode, which flushes all data to one or more files and can specify the size of each file to write to.
It will return an error when writing a directory, the file does not exist or there is not enough space, and the incomplete file will be discarded.

### Delete
BanyanDB provides a batch-deleting operation, which can delete several files at once. it will return an error if the directory does not exist or the file not reading or writing.

### Read
For reading operation, two read methods are provided:
Reading a specified location of data, which relies on a specified offset and a buffer.
Read the entire file, BanyanDB provides stream reading, which can use when the file is too large, the size gets each time can be set when using stream reading.
If entering incorrect parameters such as incorrect offset or non-existent file, it will return an error.

### Rename
Rename the file and return an error if the directory exists in this directory.

### Get size
Get the file size and return an error if the file does not exist.

### Permission
When creating a file, the default owner is the user who created the file. The owner can specify the read and write permissions of the file. If not specified, the default is read and write permissions.