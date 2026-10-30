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

package sidx

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeCapacityCapsRequestLimit(t *testing.T) {
	assert.Equal(t, 0, mergeCapacity(-1))
	assert.Equal(t, 0, mergeCapacity(0))
	assert.Equal(t, 10, mergeCapacity(10))
	assert.Equal(t, mergeCapacityHint, mergeCapacity(100000))
	assert.Equal(t, mergeCapacityHint, mergeCapacity(math.MaxInt32))
}
