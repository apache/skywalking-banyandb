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

package plan

import (
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
	"github.com/apache/skywalking-banyandb/pkg/test/leakcheck"
)

// TestMain fails the package when a test leaves a batch pinning borrowed memory, such as a
// decoded columnar frame.
func TestMain(m *testing.M) {
	leakcheck.Main(m, leakcheck.Counter("outstanding owner pins", vectorized.OutstandingOwnerPins))
}

func encodeTestFrame(b *vectorized.RecordBatch) ([]byte, error) {
	return frame.Encode(b)
}
