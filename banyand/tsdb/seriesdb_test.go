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
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestEntity(t *testing.T) {
	tester := assert.New(t)
	entity := Entity{
		Entry("productpage"),
		Entry("10.0.0.1"),
		Entry(convert.Uint64ToBytes(0)),
	}
	entity = entity.Prepend(Entry("segment"))
	tester.Equal(Entity{
		Entry("segment"),
		Entry("productpage"),
		Entry("10.0.0.1"),
		Entry(convert.Uint64ToBytes(0)),
	}, entity)
}

func TestNewPath(t *testing.T) {
	tester := assert.New(t)
	tests := []struct {
		name   string
		entity Entity
		scope  Entry
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
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
				offset: 24,
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
				seekKey: bytes.Join([][]byte{
					zeroIntBytes,
					zeroIntBytes,
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					zeroIntBytes,
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					zeroIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
				offset: 0,
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
					Hash([]byte("productpage")),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					zeroIntBytes,
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					zeroIntBytes,
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					zeroIntBytes,
					maxIntBytes,
				}, nil),
				offset: 8,
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
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					zeroIntBytes,
				}, nil),
				offset: 16,
			},
		},
		{
			name: "prepend a scope",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			scope: Entry("segment"),
			want: Path{
				isFull: true,
				prefix: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
				offset: 32,
			},
		},
		{
			name: "prepend a scope to the entity whose first entry is anyone",
			entity: Entity{
				AnyEntry,
				Entry("10.0.0.1"),
				Entry(convert.Uint64ToBytes(0)),
			},
			scope: Entry("segment"),
			want: Path{
				prefix: Hash([]byte("segment")),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("segment")),
					zeroIntBytes,
					zeroIntBytes,
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("segment")),
					zeroIntBytes,
					Hash([]byte("10.0.0.1")),
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					zeroIntBytes,
					maxIntBytes,
					maxIntBytes,
				}, nil),
				offset: 8,
			},
		},
		{
			name: "prepend a scope to the entity whose second entry is anyone",
			entity: Entity{
				Entry("productpage"),
				AnyEntry,
				Entry(convert.Uint64ToBytes(0)),
			},
			scope: Entry("segment"),
			want: Path{
				prefix: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					zeroIntBytes,
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					zeroIntBytes,
					Hash(convert.Uint64ToBytes(0)),
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					zeroIntBytes,
					maxIntBytes,
				}, nil),
				offset: 16,
			},
		},
		{
			name: "prepend a scope to the entity whose last entry is anyone",
			entity: Entity{
				Entry("productpage"),
				Entry("10.0.0.1"),
				AnyEntry,
			},
			scope: Entry("segment"),
			want: Path{
				prefix: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("segment")),
					Hash([]byte("productpage")),
					Hash([]byte("10.0.0.1")),
					zeroIntBytes,
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					maxIntBytes,
					maxIntBytes,
					zeroIntBytes,
				}, nil),
				offset: 24,
			},
		},
		{
			name: "prepend a scope to any",
			entity: Entity{
				AnyEntry,
			},
			scope: Entry("segment"),
			want: Path{
				prefix: bytes.Join([][]byte{
					Hash([]byte("segment")),
				}, nil),
				seekKey: bytes.Join([][]byte{
					Hash([]byte("segment")),
					zeroIntBytes,
				}, nil),
				template: bytes.Join([][]byte{
					Hash([]byte("segment")),
					zeroIntBytes,
				}, nil),
				mask: bytes.Join([][]byte{
					maxIntBytes,
					zeroIntBytes,
				}, nil),
				offset: 8,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewPath(tt.entity)
			if tt.scope != nil {
				got = got.Prepend(tt.scope)
			}
			tester.Equal(tt.want, got)
		})
	}
}

func Test_SeriesDatabase_Get_GetByID(t *testing.T) {
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
		Level: flags.LogLevel,
	}))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, deferFunc := test.Space(require.New(t))
			defer deferFunc()
			s, err := newSeriesDataBase(context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")), 0, dir, nil)
			tester.NoError(err)
			for _, entity := range tt.entities {
				evv := toEntityValues(entity)
				series, err := s.Get(HashEntity(entity), evv)
				tester.NoError(err)
				tester.Greater(uint(series.ID()), uint(0))
				literal := series.String()
				if literal != "" {
					tester.Equal(evv.String(), literal)
				}
				series, err = s.GetByID(series.ID())
				tester.NoError(err)
				literal = series.String()
				if literal != "" {
					tester.Equal(evv.String(), literal)
				}
			}
		})
	}
}

func Test_SeriesDatabase_List(t *testing.T) {
	tester := assert.New(t)
	tester.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	}))
	dir, deferFunc := test.Space(require.New(t))
	defer deferFunc()
	s, err := newSeriesDataBase(context.WithValue(context.Background(), logger.ContextKey, logger.GetLogger("test")), 0, dir, nil)
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
				newMockSeries(data[0].id, s.(*seriesDB)),
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
				newMockSeries(data[1].id, s.(*seriesDB)),
				newMockSeries(data[2].id, s.(*seriesDB)),
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
				newMockSeries(data[0].id, s.(*seriesDB)),
				newMockSeries(data[1].id, s.(*seriesDB)),
				newMockSeries(data[2].id, s.(*seriesDB)),
				newMockSeries(data[3].id, s.(*seriesDB)),
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
				newMockSeries(data[0].id, s.(*seriesDB)),
				newMockSeries(data[1].id, s.(*seriesDB)),
				newMockSeries(data[3].id, s.(*seriesDB)),
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
				newMockSeries(data[2].id, s.(*seriesDB)),
				newMockSeries(data[4].id, s.(*seriesDB)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			series, err := s.List(context.Background(), tt.path)
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
		d.id = common.SeriesID(convert.BytesToUint64(Hash(HashEntity(d.entity))))
		series, err := db.Get(HashEntity(d.entity), toEntityValues(d.entity))
		t.NoError(err)
		t.Greater(uint(series.ID()), uint(0))
	}
	return data
}

func toEntityValues(entity Entity) (result EntityValues) {
	for i, e := range entity {
		if len(e) == 8 && i == len(entity)-1 {
			result = append(result, Int64Value(int64(convert.BytesToUint64(e))))
		} else {
			result = append(result, StrValue(string(e)))
		}
	}
	return
}

func newMockSeries(id common.SeriesID, blockDB *seriesDB) *series {
	return newSeries(context.TODO(), id, "", blockDB)
}

func transform(list SeriesList) (seriesIDs []common.SeriesID) {
	sort.Sort(list)
	for _, s := range list {
		seriesIDs = append(seriesIDs, s.ID())
	}
	return seriesIDs
}
