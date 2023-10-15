// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package fs (file system) is an independent component to operate file and directory.
package fs

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// LocalFileSystem implements the File System interface.
type LocalFileSystem struct {
	logger *logger.Logger
}

// LocalDirectory implements the Dir interface.
type LocalDirectory struct {
	dir *os.File
}

// LocalFile implements the File interface.
type LocalFile struct {
	file *os.File
}

// NewLocalFileSystem is used to create the Local File system.
func NewLocalFileSystem() FileSystem {
	return &LocalFileSystem{
		logger: logger.GetLogger(moduleName),
	}
}

// CreateDirectory is used to create and open the directory by specified name and mode.
func (fs *LocalFileSystem) CreateDirectory(name string, permission Mode) (Dir, error) {
	var err error
	err = os.MkdirAll(name, os.FileMode(permission))
	if err != nil {
		if os.IsPermission(err) {
			return nil, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, directory name: %s, permission: %d, error message: %s", name, permission, err),
			}
		}
		return nil, &FileSystemError{
			Code:    otherError,
			Message: fmt.Sprintf("Create directory return error, directory name: %s, error message: %s", name, err),
		}
	}

	dir, err := os.Open(name)
	if err != nil {
		return nil, &FileSystemError{
			Code:    openError,
			Message: fmt.Sprintf("Open directory return error, directory name: %s, error message: %s", name, err),
		}
	}

	return &LocalDirectory{
		dir: dir,
	}, nil
}

// CreateFile is used to create and open the file by specified name and mode.
func (fs *LocalFileSystem) CreateFile(name string, permission Mode) (File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	if err != nil {
		if os.IsExist(err) {
			return nil, &FileSystemError{
				Code:    isExistError,
				Message: fmt.Sprintf("File is exist, file name: %s,error message: %s", name, err),
			}
		} else if os.IsPermission(err) {
			return nil, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, permission: %d,error message: %s", name, permission, err),
			}
		} else {
			return nil, &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Create file return error, file name: %s,error message: %s", name, err),
			}
		}
	}

	return &LocalFile{
		file: file,
	}, nil
}

// FlushWriteFile is Flush mode, which flushes all data to one file.
func (fs *LocalFileSystem) FlushWriteFile(buffer []byte, name string, permission Mode) (int, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.FileMode(permission))
	if err != nil {
		if os.IsExist(err) {
			return 0, &FileSystemError{
				Code:    isExistError,
				Message: fmt.Sprintf("File is exist, file name: %s,error message: %s", name, err),
			}
		} else if os.IsPermission(err) {
			return 0, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, permission: %d,error message: %s", name, permission, err),
			}
		} else {
			return 0, &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Create file return error, file name: %s,error message: %s", name, err),
			}
		}
	}
	defer file.Close()

	size, err := file.Write(buffer)
	if err != nil {
		return size, &FileSystemError{
			Code:    flushError,
			Message: fmt.Sprintf("Flush file return error, file name: %s,error message: %s", name, err),
		}
	}

	return size, nil
}

// DeleteDirectory is used for deleting the directory.
func (dir *LocalDirectory) DeleteDirectory() error {
	err := os.RemoveAll(dir.dir.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("Directory is not exist, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		} else if os.IsPermission(err) {
			return &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		} else {
			return &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Delete directory error, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		}
	}
	return nil
}

// ReadDirectory is used to get all lists of files or children's directories in the directory.
func (dir *LocalDirectory) ReadDirectory() ([]os.DirEntry, error) {
	dirs, err := os.ReadDir(dir.dir.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("Directory is not exist, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		} else if os.IsPermission(err) {
			return nil, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		} else {
			return nil, &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Read directory error, directory name: %s, error message: %s", dir.dir.Name(), err),
			}
		}
	}
	return dirs, nil
}

// CloseDirectory is used to close directory.
func (dir *LocalDirectory) CloseDirectory() error {
	err := dir.dir.Close()
	if err != nil {
		return &FileSystemError{
			Code:    closeError,
			Message: fmt.Sprintf("Close directory error, directory name: %s, error message: %s", dir.dir.Name(), err),
		}
	}
	return nil
}

// AppendWriteFile is append mode, which adds new data to the end of a file.
func (file *LocalFile) AppendWriteFile(buffer []byte) (int, error) {
	size, err := file.file.Write(buffer)
	if err != nil {
		if os.IsNotExist(err) {
			return size, &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else if os.IsPermission(err) {
			return size, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else {
			// include io.ErrShortWrite
			return size, &FileSystemError{
				Code:    writeError,
				Message: fmt.Sprintf("Write file error, file name: %s, error message: %s", file.file.Name(), err),
			}
		}
	}
	return size, nil
}

// AppendWritevFile is vector Append mode, which supports appending consecutive buffers to the end of the file.
// TODO: Optimizing under Linux.
func (file *LocalFile) AppendWritevFile(iov *[][]byte) (int, error) {
	var size int
	for _, buffer := range *iov {
		wsize, err := file.file.Write(buffer)
		if err != nil {
			if os.IsNotExist(err) {
				return size, &FileSystemError{
					Code:    isNotExistError,
					Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
				}
			} else if os.IsPermission(err) {
				return size, &FileSystemError{
					Code:    permissionError,
					Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
				}
			} else {
				// include io.ErrShortWrite
				return size, &FileSystemError{
					Code:    writeError,
					Message: fmt.Sprintf("Write file error, file name: %s, error message: %s", file.file.Name(), err),
				}
			}
		}
		size += wsize
	}
	return size, nil
}

// DeleteFile is used to delete the file.
func (file *LocalFile) DeleteFile() error {
	err := os.Remove(file.file.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else if os.IsPermission(err) {
			return &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else {
			return &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Delete file error, file name: %s, error message: %s", file.file.Name(), err),
			}
		}
	}
	return nil
}

// ReadFile is used to read a specified location of file.
func (file *LocalFile) ReadFile(offset int64, buffer []byte) (int, error) {
	rsize, err := file.file.ReadAt(buffer, offset)
	if err != nil {
		if err == io.EOF {
			return rsize, err
		} else if os.IsNotExist(err) {
			return rsize, &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else if os.IsPermission(err) {
			return rsize, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else {
			return rsize, &FileSystemError{
				Code:    readError,
				Message: fmt.Sprintf("Read file error, file name: %s, read file size: %d, error message: %s", file.file.Name(), rsize, err),
			}
		}
	}

	return rsize, nil
}

// ReadvFile is used to read contiguous regions of a file and disperse them into discontinuous buffers.
// TODO: Optimizing under Linux.
func (file *LocalFile) ReadvFile(offset int64, iov *[][]byte) (int, error) {
	var size int
	for _, buffer := range *iov {
		rsize, err := file.file.ReadAt(buffer, offset)
		if err != nil {
			if err == io.EOF {
				return size, err
			} else if os.IsNotExist(err) {
				return size, &FileSystemError{
					Code:    isNotExistError,
					Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
				}
			} else if os.IsPermission(err) {
				return size, &FileSystemError{
					Code:    permissionError,
					Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
				}
			} else {
				return size, &FileSystemError{
					Code:    readError,
					Message: fmt.Sprintf("Read file error, file name: %s, error message: %s", file.file.Name(), err),
				}
			}
		}
		size += rsize
		offset += int64(rsize)
	}

	return size, nil
}

// StreamReadFile is used to read the entire file using streaming read.
func (file *LocalFile) StreamReadFile(buffer []byte) (*Iter, error) {
	reader := bufio.NewReader(file.file)
	return &Iter{reader: reader, buffer: buffer, fileName: file.file.Name()}, nil
}

// GetFileSize is used to get the file written data's size and return an error if the file does not exist. The unit of file size is Byte.
func (file *LocalFile) GetFileSize() (int64, error) {
	fileInfo, err := os.Stat(file.file.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return -1, &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else if os.IsPermission(err) {
			return -1, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", file.file.Name(), err),
			}
		} else {
			return -1, &FileSystemError{
				Code:    otherError,
				Message: fmt.Sprintf("Get file size error, file name: %s, error message: %s", file.file.Name(), err),
			}
		}
	}
	return fileInfo.Size(), nil
}

// CloseFile is used to close File.
func (file *LocalFile) CloseFile() error {
	err := file.file.Close()
	if err != nil {
		return &FileSystemError{
			Code:    closeError,
			Message: fmt.Sprintf("Close File error, directory name: %s, error message: %s", file.file.Name(), err),
		}
	}
	return nil
}

// Next is used to obtain the next data of stream.
func (iter *Iter) Next() (int, error) {
	rsize, err := iter.reader.Read(iter.buffer)
	if err != nil {
		if err == io.EOF {
			return rsize, err
		} else if os.IsNotExist(err) {
			return rsize, &FileSystemError{
				Code:    isNotExistError,
				Message: fmt.Sprintf("File is not exist, file name: %s, error message: %s", iter.fileName, err),
			}
		} else if os.IsPermission(err) {
			return rsize, &FileSystemError{
				Code:    permissionError,
				Message: fmt.Sprintf("There is not enough permission, file name: %s, error message: %s", iter.fileName, err),
			}
		} else {
			return rsize, &FileSystemError{
				Code:    readError,
				Message: fmt.Sprintf("Read file error, file name: %s, read file size: %d, error message: %s", iter.fileName, rsize, err),
			}
		}
	}
	return rsize, nil
}
