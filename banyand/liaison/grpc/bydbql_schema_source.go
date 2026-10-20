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

package grpc

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

// entityRepoSchemaSource serves BydbQL schema lookups from the liaison's watch-fed
// entity repositories, the same view the native query handlers already read for
// gating and time-range clamping. Each kind has its own repository because each
// discovery service registers a separate handler.
type entityRepoSchemaSource struct {
	measures *entityRepo
	streams  *entityRepo
	traces   *entityRepo
}

var _ bydbql.SchemaSource = (*entityRepoSchemaSource)(nil)

func (s *entityRepoSchemaSource) Measure(group, name string) (*databasev1.Measure, bool) {
	return s.measures.getMeasure(identity{name: name, group: group})
}

func (s *entityRepoSchemaSource) Stream(group, name string) (*databasev1.Stream, bool) {
	return s.streams.getStream(identity{name: name, group: group})
}

func (s *entityRepoSchemaSource) Trace(group, name string) (*databasev1.Trace, bool) {
	return s.traces.getTrace(identity{name: name, group: group})
}
