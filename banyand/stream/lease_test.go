// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package stream

import (
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

func newTestRootLease(t *testing.T, root string) *native.FileRootLease {
	t.Helper()
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := native.NewFileRootLease(lock, root)
	if err != nil {
		_ = lock.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Revoke(); _ = lock.Close() })
	return lease
}
