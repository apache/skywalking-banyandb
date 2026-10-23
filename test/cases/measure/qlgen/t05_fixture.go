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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package qlgen

import (
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

// T05FixtureName isolates schema lifecycle changes from the typed-query fixture.
const T05FixtureName = "ql_t05"

// T05OtherFixtureGroup supplies a second independently tracked measure dependency.
const T05OtherFixtureGroup = "ql_t05_second_group"

// T05FixtureSchema returns the typed raw fixture under its own lifecycle identity.
func T05FixtureSchema() *databasev1.Measure {
	schema := T03FixtureSchema()
	schema.Metadata.Name = T05FixtureName
	return schema
}

// T05FixtureWrites keeps cache lifecycle results independently specified by rows.
func T05FixtureWrites(base time.Time) []*measurev1.WriteRequest {
	writes := T03FixtureWrites(base)
	for _, request := range writes {
		request.Metadata.Name = T05FixtureName
	}
	return writes
}
