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

package measure_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

func TestTopNStream(t *testing.T) {
	type fields struct {
		n        int
		reverted bool
	}
	type args struct {
		elements []int64
	}
	tests := []struct {
		name   string
		args   args
		wants  []int64
		fields fields
	}{
		{
			name: "top 3",
			fields: fields{
				n: 3,
			},
			args: args{
				elements: []int64{1, 3, 6, 8, 4, 5},
			},
			wants: []int64{8, 6, 5},
		},
		{
			name: "bottom 3",
			fields: fields{
				n:        3,
				reverted: true,
			},
			args: args{
				elements: []int64{1, 3, 6, 8, 4, 5},
			},
			wants: []int64{1, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := measure.NewTopQueue(tt.fields.n, tt.fields.reverted)
			for _, v := range tt.args.elements {
				s.Insert(measure.NewTopElement(nil, v))
			}
			ee := s.Elements()
			got := make([]int64, 0, len(ee))
			for _, e := range ee {
				got = append(got, e.Val())
			}
			assert.Equal(t, tt.wants, got)
		})
	}
}
