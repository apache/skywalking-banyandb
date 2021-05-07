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

package queue

import (
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/internal/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const name = "storage-engine"

type Component interface {
	ComponentName() string
}

type DataSubscriber interface {
	Component
	Sub(subscriber bus.Subscriber) error
}

type DataPublisher interface {
	Component
	Pub(publisher bus.Publisher) error
}

var _ run.PreRunner = (*Local)(nil)
var _ run.Config = (*Local)(nil)

type Local struct {
	logger  *logger.Logger
	test    string
	dataBus *bus.Bus
	dps     []DataPublisher
	dss     []DataSubscriber
	repo    discovery.ServiceRepo
}

func (e *Local) FlagSet() *run.FlagSet {
	e.logger = logger.GetLogger(name)
	fs := run.NewFlagSet("storage")
	fs.StringVarP(&e.test, "storage.test", "", "a", "test config")
	return fs
}

func (e *Local) Validate() error {
	e.logger.Info("test", logger.String("val", e.test))
	return nil
}

func (e Local) Name() string {
	return name
}

func (e *Local) PreRun() error {
	var err error
	e.dataBus = bus.NewBus()
	for _, dp := range e.dps {
		err = multierr.Append(err, dp.Pub(e.dataBus))
	}
	for _, ds := range e.dss {
		err = multierr.Append(err, ds.Sub(e.dataBus))
	}
	return err
}

func (e *Local) Register(component ...Component) {
	for _, c := range component {
		if ds, ok := c.(DataSubscriber); ok {
			e.dss = append(e.dss, ds)
		}
		if ps, ok := c.(DataPublisher); ok {
			e.dps = append(e.dps, ps)
		}
	}
}
