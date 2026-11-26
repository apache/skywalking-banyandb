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

//go:build windows

package resume

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// renameFile renames the temporary file of an atomic write over its target. Replacing a file
// another process has open without FILE_SHARE_DELETE, as Go opens files, fails with a sharing
// violation or access denied until that process closes it: it is tried again for a moment.
func renameFile(from, to string) error {
	return retryInUse(func() error { return os.Rename(from, to) }, fileInUse)
}

// removeFile removes a file, trying again for a moment while another process has it open.
func removeFile(path string) error {
	return retryInUse(func() error { return os.Remove(path) }, fileInUse)
}

// fileInUse reports whether a failed rename or removal is one of a file another process has open.
func fileInUse(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_ACCESS_DENIED)
}
