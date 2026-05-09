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

package fs

// testHookAfterTmpFsync is overridden by tests to simulate a crash between
// WriteAtomic's fsync(tmp) and rename. Because this file ends in _test.go,
// the indirection is removed at production link time — production binaries
// see only the no-op invokeTestHookAfterTmpFsync defined in
// local_file_system.go.
var testHookAfterTmpFsync = func() {}

func init() {
	invokeTestHookAfterTmpFsync = func() { testHookAfterTmpFsync() }
}
