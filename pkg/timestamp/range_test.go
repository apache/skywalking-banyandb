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

package timestamp

import "testing"

func Test_findRange(t *testing.T) {
	type args struct {
		timestamps []int64
		min        int64
		max        int64
	}
	tests := []struct {
		name      string
		args      args
		wantStart int
		wantEnd   int
		wantExist bool
	}{
		{
			name: "Test with empty timestamps",
			args: args{
				timestamps: []int64{},
				min:        1,
				max:        10,
			},
			wantStart: -1,
			wantEnd:   -1,
			wantExist: false,
		},
		{
			name: "Test with single timestamp",
			args: args{
				timestamps: []int64{1},
				min:        1,
				max:        1,
			},
			wantStart: 0,
			wantEnd:   0,
			wantExist: true,
		},
		{
			name: "Test with range not in timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        6,
				max:        10,
			},
			wantStart: -1,
			wantEnd:   -1,
			wantExist: false,
		},
		{
			name: "Test with range in timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        2,
				max:        4,
			},
			wantStart: 1,
			wantEnd:   3,
			wantExist: true,
		},
		{
			name: "Test with range in timestamps desc",
			args: args{
				timestamps: []int64{5, 4, 3, 2, 1},
				min:        2,
				max:        4,
			},
			wantStart: 1,
			wantEnd:   3,
			wantExist: true,
		},
		{
			name: "Test with range as timestamps",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        1,
				max:        5,
			},
			wantStart: 0,
			wantEnd:   4,
			wantExist: true,
		},
		{
			name: "Test with max equals to the first timestamp",
			args: args{
				timestamps: []int64{1, 2, 3, 4, 5},
				min:        1,
				max:        1,
			},
			wantStart: 0,
			wantEnd:   0,
			wantExist: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, gotExist := FindRange(tt.args.timestamps, tt.args.min, tt.args.max)
			if gotStart != tt.wantStart {
				t.Errorf("findRange() gotStart = %v, want %v", gotStart, tt.wantStart)
			}
			if gotEnd != tt.wantEnd {
				t.Errorf("findRange() gotEnd = %v, want %v", gotEnd, tt.wantEnd)
			}
			if gotExist != tt.wantExist {
				t.Errorf("findRange() gotExist = %v, want %v", gotExist, tt.wantExist)
			}
		})
	}
}
