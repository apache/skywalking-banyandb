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

package tsdb

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func TestNewPath(t *testing.T) {
	tester := assert.New(t)
	tests := []struct {
		name   string
		entity Entity
		want   Path
	}{
		{
			name: "general path",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			want: Path{
				isFull: true,
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the first is anyone",
			entity: Entity{
				AnyEntry,
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			want: Path{
				prefix: []byte{},
				template: bytes.Join([][]byte{
					zeroIntBytes,
					hash([]byte("10.0.0.1")),
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					zeroIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the second is anyone",
			entity: Entity{
				Entry("productpage"),
				AnyEntry,
				Entry(convert.Uint64ToBytes(0)),
			},
			want: Path{
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					zeroIntBytes,
					hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					zeroIntBytes,
					maxIntBytes,
				}, nil),
			},
		},
		{
			name: "the last is anyone",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				AnyEntry,
			},
			want: Path{
				prefix: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
				}, nil),
				template: bytes.Join([][]byte{
					hash([]byte("productpage")),
					hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					zeroIntBytes,
				}, nil),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewPath(tt.entity)
			tester.Equal(tt.want, got)
		})
	}
}

func Test_SeriesDatabase_Get(t *testing.T) {

	tests := []struct {
		name     string
		entities []Entity
	}{
		{
			name: "general entity",
			entities: []Entity{{
				Entry("productpage"),
				Entry("10.0.0.1"),
				convert.Uint64ToBytes(0),
			}},
		},
		{
			name: "duplicated entity",
			entities: []Entity{
				{
					Entry("productpage"),
					Entry("10.0.0.1"),
					convert.Uint64ToBytes(0),
				},
				{
					Entry("productpage"),
					Entry("10.0.0.1"),
					convert.Uint64ToBytes(0),
				},
			},
		},
	}
	tester := assert.New(t)
	tester.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	}))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, deferFunc := test.Space(tester)
			defer deferFunc()
			s, err := newSeriesDataBase(context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")), dir)
			tester.NoError(err)
			for _, entity := range tt.entities {
				series, err := s.Get(entity)
				tester.NoError(err)
				tester.Equal(hashEntity(entity), series.ID())
			}
		})
	}
}

func Test_SeriesDatabase_List(t *testing.T) {
	tester := assert.New(t)
	tester.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "debug",
	}))
	dir, deferFunc := test.Space(tester)
	defer deferFunc()
	s, err := newSeriesDataBase(context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")), dir)
	tester.NoError(err)
	data := setUpEntities(tester, s)
	tests := []struct {
		name    string
		path    Path
		wantErr bool
		want    SeriesList
	}{
		{
			name: "equal",
			path: NewPath([]Entry{
				Entry("productpage"),
				Entry("10.0.0.1"),
				convert.Uint64ToBytes(0),
			}),
			want: SeriesList{
				newMockSeries(data[0].id),
			},
		},
		{
			name: "all in an instance",
			path: NewPath([]Entry{
				Entry("productpage"),
				Entry("10.0.0.2"),
				AnyEntry,
			}),
			want: SeriesList{
				newMockSeries(data[1].id),
				newMockSeries(data[2].id),
			},
		},
		{
			name: "all in a service",
			path: NewPath([]Entry{
				Entry("productpage"),
				AnyEntry,
				AnyEntry,
			}),
			want: SeriesList{
				newMockSeries(data[0].id),
				newMockSeries(data[1].id),
				newMockSeries(data[2].id),
				newMockSeries(data[3].id),
			},
		},
		{
			name: "all successful",
			path: NewPath([]Entry{
				AnyEntry,
				AnyEntry,
				convert.Uint64ToBytes(0),
			}),
			want: SeriesList{
				newMockSeries(data[0].id),
				newMockSeries(data[1].id),
				newMockSeries(data[3].id),
			},
		},
		{
			name: "all error",
			path: NewPath([]Entry{
				AnyEntry,
				AnyEntry,
				convert.Uint64ToBytes(1),
			}),
			want: SeriesList{
				newMockSeries(data[2].id),
				newMockSeries(data[4].id),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			series, err := s.List(tt.path)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
			tester.Equal(transform(tt.want), transform(series))
		})
	}
}

type entityWithID struct {
	id     common.SeriesID
	entity Entity
}

func setUpEntities(t *assert.Assertions, db SeriesDatabase) []*entityWithID {
	data := []*entityWithID{
		{
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				convert.Uint64ToBytes(0),
			},
		},
		{
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.2"),
				convert.Uint64ToBytes(0),
			},
		},
		{
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.2"),
				convert.Uint64ToBytes(1),
			},
		},
		{
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.3"),
				convert.Uint64ToBytes(0),
			},
		},
		{
			entity: Entity{
				Entry("payment"),
				Entry("10.0.0.2"),
				convert.Uint64ToBytes(1),
			},
		},
	}
	for _, d := range data {
		d.id = common.SeriesID(convert.BytesToUint64(hash(hashEntity(d.entity))))
		series, err := db.Get(d.entity)
		t.NoError(err)
		t.Equal(hashEntity(d.entity), series.ID())
	}
	return data
}

func newMockSeries(id common.SeriesID) *series {
	return newSeries(id, nil)
}

func transform(list SeriesList) (seriesIDs []common.SeriesID) {
	sort.Sort(list)
	for _, s := range list {
		seriesIDs = append(seriesIDs, s.ID())
	}
	return seriesIDs
}
