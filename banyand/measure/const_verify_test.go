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

package measure

import (
	"flag"
	"os"

	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// configureInvariantChecks runs every test of the package with the invariants of
// ConstColumns and runs checked against every row: an entity tag carried as a constant
// must equal the tag of every row it stands for, and every bucket run must hold only
// rows of its bucket. The checks cost a pass over every batch, so a benchmark run, and
// the bench gates, which time the scan, leave them off.
func configureInvariantChecks() {
	flag.Parse()
	if bench := flag.Lookup("test.bench"); bench != nil && bench.Value.String() != "" {
		return
	}
	if os.Getenv("RUN_BENCH_GATES") == "1" {
		return
	}
	verifyEntityConstants.Store(true)
	vmeasure.SetRunsVerification(true)
}
