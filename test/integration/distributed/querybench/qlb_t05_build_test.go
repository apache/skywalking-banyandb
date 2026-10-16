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

package querybench

import (
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestQLBT05QueriesRetainTextAndChangeBindings(t *testing.T) {
	base := time.Unix(1700000000, 0)
	for _, shape := range qlbT05Shapes {
		first, initial, err := qlbT05Query(shape, 0, base)
		if err != nil {
			t.Fatal(err)
		}
		rebound, changed, err := qlbT05Query(shape, 1, base)
		if err != nil {
			t.Fatal(err)
		}
		if first.Query != rebound.Query || len(initial.DataPoints) != 32 || len(changed.DataPoints) != 32 || qlbT01Hash(initial) == qlbT01Hash(changed) {
			t.Fatal("T05 must compare one text with changed bound rows")
		}
		statement, prepareErr := bydbql.Prepare(first.Query)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		if _, bindErr := statement.Bind(first.Params); bindErr != nil {
			t.Fatal(bindErr)
		}
		if _, bindErr := statement.Bind(rebound.Params); bindErr != nil {
			t.Fatal(bindErr)
		}
		if shape == "typed_wide" && (statement.NumPlaceholders() != 9 || len(initial.DataPoints[0].Fields) != 16) {
			t.Fatal("typed planning workload requires eight bound time predicates and sixteen projections")
		}
	}
}
