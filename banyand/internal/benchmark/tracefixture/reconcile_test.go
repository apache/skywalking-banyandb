// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracefixture

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareIndexScanRowsRequiresCompleteMultiset(t *testing.T) {
	first := hashIndexQueryRow(1, 10, []byte("first"))
	second := hashIndexQueryRow(1, 20, []byte("second"))
	expectedRows := map[indexQueryRow]uint64{first: 2, second: 1}
	expectedData := map[string]struct{}{"first": {}, "second": {}}

	require.NoError(t, compareIndexScanRows(expectedRows, map[indexQueryRow]uint64{first: 2, second: 1}, expectedData,
		map[string]struct{}{"first": {}, "second": {}}))
	require.ErrorContains(t, compareIndexScanRows(expectedRows, map[indexQueryRow]uint64{first: 1, second: 1}, expectedData,
		map[string]struct{}{"first": {}, "second": {}}), "count mismatch")
}

func TestCompareIndexKeyedRowsRequiresEveryUniqueDataValue(t *testing.T) {
	first := hashIndexQueryRow(1, 10, []byte("first"))
	duplicateFirst := hashIndexQueryRow(2, 20, []byte("first"))
	second := hashIndexQueryRow(1, 30, []byte("second"))
	expectedRows := map[indexQueryRow]uint64{first: 1, duplicateFirst: 2, second: 1}
	expectedData := map[string]struct{}{"first": {}, "second": {}}

	require.NoError(t, compareIndexKeyedRows(expectedRows, map[indexQueryRow]uint64{duplicateFirst: 2, second: 1}, expectedData,
		map[string]struct{}{"first": {}, "second": {}}))
	require.ErrorContains(t, compareIndexKeyedRows(expectedRows, map[indexQueryRow]uint64{first: 1}, expectedData,
		map[string]struct{}{"first": {}}), "data count mismatch")
	require.ErrorContains(t, compareIndexKeyedRows(expectedRows, map[indexQueryRow]uint64{duplicateFirst: 3, second: 1}, expectedData,
		map[string]struct{}{"first": {}, "second": {}}), "exceeding its physical count")
}
