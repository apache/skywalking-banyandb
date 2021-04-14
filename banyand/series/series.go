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

package series

import (
	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const name = "series"

var (
	_ bus.MessageListener    = (*Series)(nil)
	_ run.PreRunner          = (*Series)(nil)
	_ storage.DataSubscriber = (*Series)(nil)
)

type Series struct {
	log *logger.Logger
}

func (s Series) ComponentName() string {
	return name
}

func (s *Series) Sub(subscriber bus.Subscriber) error {
	return subscriber.Subscribe(storage.TraceData, s)
}

func (s Series) Name() string {
	return name
}

func (s *Series) PreRun() error {
	s.log = logger.GetLogger(name)
	return nil
}

func (s Series) Rev(message bus.Message) {
	s.log.Info("rev", logger.Any("msg", message.Data()))
}
