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

package vectorized

// ColumnRole identifies the semantic role of a column within a RecordBatch.
type ColumnRole int

// Column roles. Each batch schema may include at most one column per metadata role.
const (
	RoleTimestamp ColumnRole = iota
	RoleVersion
	RoleSeriesID
	RoleShardID
	RoleTag
	RoleField
)

// ColumnDef describes one column in a BatchSchema.
type ColumnDef struct {
	Name      string
	TagFamily string
	Role      ColumnRole
	Type      ColumnType
}

// tagKey is the composite key used to index tag columns by (family, name).
// Using a struct key eliminates two issues with a string-concatenated key:
//   - it cannot collide when family or name contains the separator character;
//   - the lookup avoids allocating a fresh string on every hot-path call.
type tagKey struct {
	family string
	name   string
}

// BatchSchema is the immutable column layout shared by every RecordBatch in a pipeline.
type BatchSchema struct {
	tagByPath    map[tagKey]int
	fieldByName  map[string]int
	Columns      []ColumnDef
	timestampIdx int
	versionIdx   int
	seriesIDIdx  int
	shardIDIdx   int
}

// NewBatchSchema builds a BatchSchema and precomputes lookup indices.
func NewBatchSchema(cols []ColumnDef) *BatchSchema {
	s := &BatchSchema{
		Columns:      cols,
		timestampIdx: -1,
		versionIdx:   -1,
		seriesIDIdx:  -1,
		shardIDIdx:   -1,
		tagByPath:    make(map[tagKey]int),
		fieldByName:  make(map[string]int),
	}
	for i, c := range cols {
		switch c.Role {
		case RoleTimestamp:
			s.timestampIdx = i
		case RoleVersion:
			s.versionIdx = i
		case RoleSeriesID:
			s.seriesIDIdx = i
		case RoleShardID:
			s.shardIDIdx = i
		case RoleTag:
			s.tagByPath[tagKey{family: c.TagFamily, name: c.Name}] = i
		case RoleField:
			s.fieldByName[c.Name] = i
		}
	}
	return s
}

// TimestampIndex returns the timestamp column index, or -1 if absent.
func (s *BatchSchema) TimestampIndex() int { return s.timestampIdx }

// VersionIndex returns the version column index, or -1 if absent.
func (s *BatchSchema) VersionIndex() int { return s.versionIdx }

// SeriesIDIndex returns the series-id column index, or -1 if absent.
func (s *BatchSchema) SeriesIDIndex() int { return s.seriesIDIdx }

// ShardIDIndex returns the shard-id column index, or -1 if absent.
func (s *BatchSchema) ShardIDIndex() int { return s.shardIDIdx }

// TagIndex returns the column index for a (family, name) tag.
// Lookup uses a struct key, so it does not allocate.
func (s *BatchSchema) TagIndex(family, name string) (int, bool) {
	i, ok := s.tagByPath[tagKey{family: family, name: name}]
	return i, ok
}

// FieldIndex returns the column index for a field name.
func (s *BatchSchema) FieldIndex(name string) (int, bool) {
	i, ok := s.fieldByName[name]
	return i, ok
}
