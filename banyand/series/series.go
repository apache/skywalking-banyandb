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

//go:generate mockgen -destination=./series_mock.go -package=series . UniModel
package series

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
	posting2 "github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// TraceState represents the State of a traceSeries link
type TraceState int

const (
	TraceStateDefault TraceState = iota
	TraceStateSuccess
	TraceStateError
)

//ScanOptions contain options
//nolint
type ScanOptions struct {
	Projection []string
	State      TraceState
	Limit      uint32
}

//TraceRepo contains traceSeries and entity data
type TraceRepo interface {
	//FetchTrace returns data.Trace by traceID
	FetchTrace(traceSeries common.Metadata, traceID string, opt ScanOptions) (data.Trace, error)
	//FetchEntity returns data.Entity by ChunkID
	FetchEntity(traceSeries common.Metadata, shardID uint, chunkIDs posting2.List, opt ScanOptions) ([]data.Entity, error)
	//ScanEntity returns data.Entity between a duration by ScanOptions
	ScanEntity(traceSeries common.Metadata, startTime, endTime uint64, opt ScanOptions) ([]data.Entity, error)
	// Write entity to the given traceSeries
	Write(traceSeries common.Metadata, ts time.Time, seriesID, entityID string, dataBinary []byte, items ...interface{}) (bool, error)
}

//UniModel combines Trace, Metric and Log repositories into a union interface
type UniModel interface {
	TraceRepo
}

//SchemaRepo contains schema definition
type SchemaRepo interface {
	TraceSeries() schema.TraceSeries
	IndexRule() schema.IndexRule
	IndexRuleBinding() schema.IndexRuleBinding
}

type IndexObjectFilter func(object *v1.IndexObject) bool

//IndexFilter provides methods to find a specific index related objects
type IndexFilter interface {
	//IndexRules fetches v1.IndexRule by Series defined in IndexRuleBinding and a filter
	IndexRules(ctx context.Context, subject *v1.Series, filter IndexObjectFilter) ([]*v1.IndexRule, error)
}

//Service provides operations how to access series module
type Service interface {
	UniModel
	SchemaRepo
	IndexFilter
	run.PreRunner
	run.Service
}
