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

package shard

import (
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ bus.MessageListener = (*Shard)(nil)

type Shard struct {
	log *logger.Logger
	bus *bus.Bus
}

func NewShard(bus *bus.Bus) *Shard {
	return &Shard{
		bus: bus,
		log: logger.Log.Scope("shard"),
	}
}

func (s Shard) Rev(message bus.Message) {
	s.log.Sugar().Infow("rev", "msg", message.Data())
	_ = s.bus.Publish(storage.TraceSharded, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), "sharded message"))
}

func (s Shard) Close() error {
	s.log.Sugar().Infow("closed")
	return nil
}
