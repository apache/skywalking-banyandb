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
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/fs"
)

func TestDeletableEpochs(t *testing.T) {
	tests := []struct {
		name            string
		knownEpochs     []uint64
		deletableEpochs []uint64
		n               int
	}{
		{
			name:            "empty",
			n:               1,
			knownEpochs:     nil,
			deletableEpochs: nil,
		},
		{
			name:            "one",
			n:               1,
			knownEpochs:     []uint64{1},
			deletableEpochs: nil,
		},
		{
			name:            "many",
			n:               1,
			knownEpochs:     []uint64{1, 2, 3, 4},
			deletableEpochs: []uint64{1, 2, 3},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s-%d", test.name, test.n), func(t *testing.T) {
			var gc garbageCleaner
			gc.init(&sidx{
				fileSystem: fs.NewLocalFileSystem(),
			})

			for _, epoch := range test.knownEpochs {
				gc.registerSnapshot(&snapshot{epoch: epoch})
			}
			if !reflect.DeepEqual(gc.deletableEpochs, test.deletableEpochs) {
				t.Errorf("expected deletable: %#v, got %#v", test.deletableEpochs, gc.deletableEpochs)
			}
		})
	}
}
