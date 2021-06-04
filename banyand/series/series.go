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

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type Trace interface {
	FetchTrace(traceID string) (data.Trace, error)
	FetchEntity(chunkIDs []string, fields []string) ([]data.Entity, error)
	ScanEntity(startTime, endTime uint64, fields []string) ([]data.Entity, error)
}

type UniModel interface {
	Trace
}

type Service interface {
	UniModel
	run.PreRunner
}

func NewService(ctx context.Context, db storage.Database) (Service, error) {
	return nil, nil
}

// TODO: this interface should contains methods to access schema objects
type SchemaRepo interface {
}
