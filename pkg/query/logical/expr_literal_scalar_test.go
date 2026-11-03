// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership.  Apache Software Foundation (ASF) licenses this file to you
// under the Apache License, Version 2.0 (the "License"); you may
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

package logical

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A scalar literal compared against another scalar literal must be compared by
// VALUE. These expressions carry no identity, so a pointer comparison only
// succeeds when both sides happen to be the same allocation — which is what
// made BINARY_OP_MATCH and BINARY_OP_HAVING return nothing for scalar tags.
//
// The array branches already used slices.Contains (a value comparison); only the
// scalar branches compared pointers.

func TestStrLiteralContainsComparesValues(t *testing.T) {
	stored := &strLiteral{"ERROR"}
	query := &strLiteral{"ERROR"}
	require.NotSame(t, stored, query, "the test is only meaningful for distinct allocations")
	require.True(t, stored.Contains(query), "equal strings must match regardless of allocation")
	require.False(t, stored.Contains(&strLiteral{"INFO"}))
}

func TestStrLiteralBelongToComparesValues(t *testing.T) {
	stored := &strLiteral{"ERROR"}
	require.True(t, stored.BelongTo(&strLiteral{"ERROR"}), "equal strings must belong regardless of allocation")
	require.False(t, stored.BelongTo(&strLiteral{"INFO"}))
}

func TestInt64LiteralContainsComparesValues(t *testing.T) {
	stored := &int64Literal{1500}
	query := &int64Literal{1500}
	require.NotSame(t, stored, query)
	require.True(t, stored.Contains(query), "equal ints must match regardless of allocation")
	require.False(t, stored.Contains(&int64Literal{1501}))
}

func TestInt64LiteralBelongToComparesValues(t *testing.T) {
	stored := &int64Literal{1500}
	require.True(t, stored.BelongTo(&int64Literal{1500}), "equal ints must belong regardless of allocation")
	require.False(t, stored.BelongTo(&int64Literal{1501}))
}

// The array branches must keep working: a scalar contained in a one-element
// array, and the array-membership cases.
func TestLiteralContainmentAgainstArraysUnchanged(t *testing.T) {
	require.True(t, (&strLiteral{"a"}).Contains(&strArrLiteral{arr: []string{"a"}}))
	require.False(t, (&strLiteral{"a"}).Contains(&strArrLiteral{arr: []string{"b"}}))
	// A scalar belongs to an array only when it is the sole element; that is the
	// existing contract, not a value-comparison concern.
	require.True(t, (&strArrLiteral{arr: []string{"a"}}).BelongTo(&strLiteral{"a"}))
	require.False(t, (&strArrLiteral{arr: []string{"a", "b"}}).BelongTo(&strLiteral{"a"}))
	require.True(t, (&int64Literal{7}).Contains(&int64ArrLiteral{arr: []int64{7}}))
	require.True(t, (&int64ArrLiteral{arr: []int64{7}}).BelongTo(&int64Literal{7}))
}

// A literal must still not match an unrelated type or a null.
func TestLiteralContainmentRejectsUnrelatedTypes(t *testing.T) {
	require.False(t, (&strLiteral{"a"}).Contains(&int64Literal{1}))
	require.False(t, (&int64Literal{1}).Contains(&strLiteral{"1"}))
	require.False(t, (&strLiteral{"a"}).Contains((&nullLiteral{})))
}

// Equal must stay value-based and must now agree with Contains.
func TestEqualAndContainsAgreeForScalars(t *testing.T) {
	require.True(t, (&strLiteral{"a"}).Equal(&strLiteral{"a"}))
	require.True(t, (&strLiteral{"a"}).Contains(&strLiteral{"a"}))
	require.True(t, (&int64Literal{1}).Equal(&int64Literal{1}))
	require.True(t, (&int64Literal{1}).Contains(&int64Literal{1}))
}
