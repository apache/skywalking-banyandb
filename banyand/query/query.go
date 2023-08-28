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

// Package query implement the query module for liaison and other modules to retrieve data.
package query

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// NewService return a new query service.
func NewService(_ context.Context, streamService stream.Service, measureService measure.Service,
	metaService metadata.Repo, pipeline queue.Server,
) (run.Unit, error) {
	svc := &queryService{
		metaService: metaService,
		pipeline:    pipeline,
	}
	// measure query processor
	svc.mqp = &measureQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	// stream query processor
	svc.sqp = &streamQueryProcessor{
		streamService: streamService,
		queryService:  svc,
	}
	// topN query processor
	svc.tqp = &topNQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	return svc, nil
}
