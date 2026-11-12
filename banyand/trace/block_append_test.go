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

package trace

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// TestFullTagAppendKeepsExistingTagsWhenATagIsAdded pins that appending a block that adds a tag keeps every tag at
// one value per span; growing the destination slice used to strand the values appended through pointers taken
// before the growth.
func TestFullTagAppendKeepsExistingTagsWhenATagIsAdded(t *testing.T) {
	newTag := func(name string) tag {
		return tag{name: name, valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte(name)}}
	}
	bi := &blockPointer{}
	bi.spans = [][]byte{[]byte("s1")}
	// A clipped slice is full, so adding a tag reallocates it.
	bi.tags = slices.Clip([]tag{newTag("a"), newTag("c")})
	b := &blockPointer{}
	b.spans = [][]byte{[]byte("s2")}
	b.tags = []tag{newTag("a"), newTag("b"), newTag("c")}
	fullTagAppend(bi, b, 1)
	for _, tg := range bi.tags {
		require.Lenf(t, tg.values, 2, "tag %s", tg.name)
	}
}
