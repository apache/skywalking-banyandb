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
	"io"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// Queue builds a data transmission tunnel between subscribers and publishers.
//
//go:generate mockgen -destination=./queue_mock.go -package=queue github.com/apache/skywalking-banyandb/pkg/bus MessageListener
type Queue interface {
	Client
	Server
	run.Service
}

// Client is the interface for publishing data to the queue.
//
//go:generate mockgen -destination=./pipeline_mock.go -package=queue . Client
type Client interface {
	run.Unit
	bus.Publisher
	bus.Broadcaster
	NewBatchPublisher() BatchPublisher
	Register(schema.EventHandler)
}

// Server is the interface for receiving data from the queue.
type Server interface {
	run.Unit
	bus.Subscriber
	GetPort() *uint32
}

// BatchPublisher is the interface for publishing data in batch.
type BatchPublisher interface {
	bus.Publisher
	io.Closer
}
