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

package storage

import (
	"time"

	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type retentionTask[T TSTable, O any] struct {
	database *database[T, O]
	expr     string
	option   cron.ParseOption
	duration time.Duration
}

func newRetentionTask[T TSTable, O any](database *database[T, O], ttl IntervalRule) *retentionTask[T, O] {
	var expr string
	switch ttl.Unit {
	case HOUR:
		// Every hour on the 5th minute
		expr = "5 *"
	case DAY:
		// Every day on 00:05
		expr = "5 0"
	}
	return &retentionTask[T, O]{
		database: database,
		option:   cron.Minute | cron.Hour,
		expr:     expr,
		duration: ttl.estimatedDuration(),
	}
}

func (rc *retentionTask[T, O]) run(now time.Time, l *logger.Logger) bool {
	shardList := rc.database.sLst.Load()
	if shardList == nil {
		return false
	}
	deadline := now.Add(-rc.duration)

	for _, shard := range *shardList {
		if err := shard.segmentController.remove(deadline); err != nil {
			l.Error().Err(err)
		}
	}
	if err := rc.database.indexController.run(deadline); err != nil {
		l.Error().Err(err)
	}
	return true
}
