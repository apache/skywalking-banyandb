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

package measure_test

import (
	"time"

	gm "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// expectLiveReservedBytes waits for the live query memory reservations of this process to return to want. A query that
// ends early, such as one with a limit, cancels its scans on the data nodes, which release what they hold shortly after
// the query returns, so the check must allow for that.
func expectLiveReservedBytes(want int64) {
	gm.Eventually(func() int64 { return vectorized.QueryMemoryStats().LiveReservedBytes }, flags.EventuallyTimeout, 10*time.Millisecond).Should(gm.Equal(want))
}
