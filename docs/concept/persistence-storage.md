# Persistence Storage
Persistence storage is used for unifying data of BanyanDB persistence, including write-ahead logging(WAL), index, and data collected from skywalking and other observability platforms or APM systems. It provides various implementations and IO modes to satisfy the need of different components.
BanyanDB provides a concise interface that shields the complexity of the implementation from the upper layer. By exposing necessary interfaces, upper components do not need to care how persistence is implemented and avoid dealing with differences between different operating systems.

# IO Mode
Persistence storage offers a range of IO modes to cater to various throughput requirements. The interface can be accessed by developers and can be configured through settings, which can be set in the configuration file.

## Io_uring
Io_uring is a new feature in Linux 5.1, which is fully asynchronous and offers high throughput. In the scene of massive storage, io_uring can bring significant benefits. The following is the diagram about how io_uring works.
![](/assets/io_uring.jpg)
If the user sets io_uring for use, the read and write requests will first be placed in the submission queue buffer when calling the operation API. When the threshold is reached, batch submissions will be made to SQ. After the kernel threads complete execution, the requests will be placed in the CQ, and the user can obtain the request results.

## Synchronous IO
The most common IO mode is Synchronous IO, but it has a relatively low throughput. BanyanDB provides a nonblocking mode that is compatible with lower Linux versions.

# Operation
## Directory
### Create
Create the specified directory and return the file descriptor, the error will happen if the directory already exists.
The following is the pseudocode that calls the API in the go style.
`CreateDirectory(name String, permission int) (int, error)`

### Open
Open the directory and return an error if the file descriptor does not exist.
The following is the pseudocode that calls the API in the go style.
`fd.OpenDirectory() (error)`

### Delete
Delete the directory and all files and return an error if the directory does not exist or the directory not reading or writing.
The following is the pseudocode that calls the API in the go style.
`fd.DeleteDirectory() (error)`

### Rename
Rename the directory and return an error if the directory already exists.
The following is the pseudocode that calls the API in the go style.
`fd.RenameDirectory(newName String) (error)`

### Get Children
Get all lists of files or children's directories in the directory and an error if the directory does not exist.
The following is the pseudocode that calls the API in the go style.
`fd.ReadDirectory() (FileList, error)`

### Permission
When creating a file, the default owner is the user who created the directory. The owner can specify read and write permissions of the directory. If not specified, the default is read and write permissions, which include permissions for all files in the directory.
The following is the pseudocode that calls the API in the go style.
`fd.SetDirectoryPermission(permission int) (error)`

## File
### Create
Create the specified file and return the file descriptor, the error will happen if the file already exists.
The following is the pseudocode that calls the API in the go style.
`CreateFile(name String, permission int) (int, error)`

### Open
Open the file and return an error if the file descriptor does not exist.
The following is the pseudocode that calls the API in the go style.
`fd.OpenFile() (error)`

### Write
BanyanDB provides two methods for writing files.
Append mode, which adds new data to the end of a file. This mode is typically used for WAL.
Flush mode, which flushes all data to one file. It will return an error when writing a directory, the file does not exist or there is not enough space, and the incomplete file will be discarded.
The following is the pseudocode that calls the API in the go style.
For append mode: `fd.AppendWriteFile(data []byte) (error)`
For flush mode:  `FlushWriteFile(data []byte, permission int) (int, error)`

### Delete
BanyanDB provides a batch-deleting operation, which can delete several files at once. it will return an error if the directory does not exist or the file not reading or writing.
The following is the pseudocode that calls the API in the go style.
`DeleteFile(fds List) (error)`

### Read
For reading operation, two read methods are provided:
Reading a specified location of data, which relies on a specified offset and a buffer.
Read the entire file, BanyanDB provides stream reading, which can use when the file is too large, the size gets each time can be set when using stream reading.
If entering incorrect parameters such as incorrect offset or non-existent file, it will return an error.
The following is the pseudocode that calls the API in the go style.
`fd.ReadFile(offset int, data []byte) (error)`

### Rename
Rename the file and return an error if the directory exists in this directory.
The following is the pseudocode that calls the API in the go style.
`fd.RenameFile(newName String) (error)`

### Get size
Get the file size and return an error if the file does not exist. The unit of file size is Byte.
The following is the pseudocode that calls the API in the go style.
`fd.GetFileSize() (int, error)`

### Permission
When creating a file, the default owner is the user who created the file. The owner can specify the read and write permissions of the file. If not specified, the default is read and write permissions.
The following is the pseudocode that calls the API in the go style.
`fd.SetFilePermission(permission int) (error)`