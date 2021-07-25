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

//go:generate mockgen -destination=./index_mock.go -package=index . Repo
package index

import (
	"context"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type Condition struct {
	Key    string
	Values [][]byte
	Op     apiv1.PairQuery_BinaryOp
}

type Repo interface {
	Search(series common.Metadata, shardID uint, startTime, endTime uint64, conditions []Condition) (posting.List, error)
}

type Builder interface {
	run.Config
	run.PreRunner
}

type Service interface {
	Repo
	Builder
}

func NewService(ctx context.Context, repo discovery.ServiceRepo, pipeline queue.Queue) (Service, error) {
	return nil, nil
}
