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

package test

import (
	"fmt"

	"github.com/golang/mock/gomock"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var (
	_ gomock.Matcher = (*shardEventMatcher)(nil)
	_ gomock.Matcher = (*entityEventMatcher)(nil)
)

type shardEventMatcher struct {
	action databasev1.Action
}

// NewShardEventMatcher return a Matcher to comparing shard event's action.
func NewShardEventMatcher(action databasev1.Action) gomock.Matcher {
	return &shardEventMatcher{
		action: action,
	}
}

func (s *shardEventMatcher) Matches(x interface{}) bool {
	if m, messageOk := x.(bus.Message); messageOk {
		if evt, dataOk := m.Data().(*databasev1.ShardEvent); dataOk {
			return evt.Action == s.action
		}
	}

	return false
}

func (s *shardEventMatcher) String() string {
	return fmt.Sprintf("shard-event-matcher(%s)", databasev1.Action_name[int32(s.action)])
}

type entityEventMatcher struct {
	action databasev1.Action
}

// NewEntityEventMatcher return a Matcher for comparing entity event's action.
func NewEntityEventMatcher(action databasev1.Action) gomock.Matcher {
	return &entityEventMatcher{
		action: action,
	}
}

func (s *entityEventMatcher) Matches(x interface{}) bool {
	if m, messageOk := x.(bus.Message); messageOk {
		if evt, dataOk := m.Data().(*databasev1.EntityEvent); dataOk {
			return evt.Action == s.action
		}
	}

	return false
}

func (s *entityEventMatcher) String() string {
	return fmt.Sprintf("entity-event-matcher(%s)", databasev1.Action_name[int32(s.action)])
}
