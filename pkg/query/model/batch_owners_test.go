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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type refOwner struct{ refs int }

func (o *refOwner) Retain()  { o.refs++ }
func (o *refOwner) Release() { o.refs-- }

func TestMeasureBatchReleaseDropsOwners(t *testing.T) {
	o := &refOwner{refs: 1}
	b := AcquireMeasureBatch(vectorized.NewBatchSchema(nil), 4)
	b.Owners.Pin(o)
	require.Equal(t, 2, o.refs)

	// A second holder pins before the first releases: the owner survives.
	var other vectorized.Owners
	other.PinAll(&b.Owners)
	require.Equal(t, 3, o.refs)
	b.Release()
	require.Equal(t, 2, o.refs, "Release drops the batch's own pin only")
	other.Release()
	require.Equal(t, 1, o.refs)

	// A pooled batch comes back with no owners.
	reused := AcquireMeasureBatch(vectorized.NewBatchSchema(nil), 4)
	require.Zero(t, reused.Owners.Len())
	reused.Release()
}
