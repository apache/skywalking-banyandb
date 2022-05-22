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

package query

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type Executor interface {
	run.PreRunner
}

func NewExecutor(_ context.Context, streamService stream.Service, measureService measure.Service,
	metaService metadata.Service, serviceRepo discovery.ServiceRepo, pipeline queue.Queue) (Executor, error) {

	svc := &queryService{
		metaService: metaService,
		serviceRepo: serviceRepo,
		pipeline:    pipeline,
	}
	svc.mqp = &measureQueryProcessor{
		measureService: measureService,
		queryService:   svc,
	}
	svc.sqp = &streamQueryProcessor{
		streamService: streamService,
		queryService:  svc,
	}
	return svc, nil
}
