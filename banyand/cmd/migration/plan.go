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

package main

import (
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
)

// catalogExecutors returns one executor per supported catalog, in the
// order the orchestrator runs them. Executors whose catalog bucket is
// empty are skipped by the orchestrator.
func catalogExecutors() []migration.CatalogExecutor {
	return []migration.CatalogExecutor{
		measure.NewMigrationExecutor(),
		stream.NewMigrationExecutor(),
	}
}
