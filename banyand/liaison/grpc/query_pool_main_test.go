// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package grpc_test

import (
	"os"
	"testing"

	"github.com/apache/skywalking-banyandb/banyand/protector"
)

// TestMain pins the host memory to "unreadable", so the query budget keeps its fixed 64 MiB fallback pool (8 MiB per
// query) and the budget tests exercise the same limits on every machine instead of a pool sized from its memory.
func TestMain(m *testing.M) {
	protector.SetHostMemoryForTest(0)
	os.Exit(m.Run())
}
