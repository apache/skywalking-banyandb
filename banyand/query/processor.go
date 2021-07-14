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
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	moduleName = "query-processor"
)

var (
	_ Executor                  = (*queryProcessor)(nil)
	_ bus.MessageListener       = (*queryProcessor)(nil)
	_ executor.ExecutionContext = (*queryProcessor)(nil)
)

type queryProcessor struct {
	index.Repo
	series.UniModel
	schemaRepo  series.SchemaRepo
	log         *logger.Logger
	serviceRepo discovery.ServiceRepo
}

func (q *queryProcessor) Rev(message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*v1.EntityCriteria)
	if !ok {
		q.log.Warn().Msg("invalid event data type")
		return
	}
	q.log.Info().
		Msg("received a query event")
	analyzer := logical.NewAnalyzer(q.schemaRepo)
	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        queryCriteria.Metadata(nil),
	}
	s, err := analyzer.BuildTraceSchema(context.TODO(), *metadata)
	if err != nil {
		return
	}

	p, err := analyzer.Analyze(context.TODO(), queryCriteria, metadata, s)
	if err != nil {
		return
	}

	entities, err := p.Execute(q)
	if err != nil {
		return
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

func (q *queryProcessor) Name() string {
	return moduleName
}

func (q *queryProcessor) PreRun() error {
	q.log = logger.GetLogger(moduleName)
	return q.serviceRepo.Subscribe(event.TopicQueryEvent, q)
}
