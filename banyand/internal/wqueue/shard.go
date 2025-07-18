// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package wqueue provides a write queue implementation for managing data shards.
package wqueue

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Shard represents a data shard containing a sub-queue and associated metadata.
type Shard[S SubQueue] struct {
	sq       S
	l        *logger.Logger
	location string
	id       common.ShardID
}

// SubQueue returns the underlying sub-queue of the shard.
func (s Shard[S]) SubQueue() S {
	return s.sq
}

// Close closes the shard and its underlying sub-queue.
func (s Shard[S]) Close() error {
	return s.sq.Close()
}
