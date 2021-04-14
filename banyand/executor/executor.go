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

package executor

import (
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const name = "executor"

var (
	_ bus.MessageListener    = (*Executor)(nil)
	_ run.PreRunner          = (*Executor)(nil)
	_ storage.DataSubscriber = (*Executor)(nil)
	_ storage.DataPublisher  = (*Executor)(nil)
)

type Executor struct {
	log       *logger.Logger
	publisher bus.Publisher
}

func (s *Executor) Pub(publisher bus.Publisher) error {
	s.publisher = publisher
	return nil
}

func (s *Executor) ComponentName() string {
	return name
}

func (s *Executor) Sub(subscriber bus.Subscriber) error {
	return subscriber.Subscribe(storage.TraceRaw, s)
}

func (s *Executor) Name() string {
	return name
}

func (s *Executor) PreRun() error {
	s.log = logger.GetLogger(name)
	return nil
}

func (s Executor) Rev(message bus.Message) {
	s.log.Info("rev", logger.Any("msg", message.Data()))
	_ = s.publisher.Publish(storage.TraceIndex, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), "index message"))
	_ = s.publisher.Publish(storage.TraceData, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), "data message"))
}
