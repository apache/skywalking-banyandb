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

package gossip

import (
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type local struct {
	local  *bus.Bus
	stopCh chan struct{}

	listeners     map[string][]MessageListener
	listenersLock sync.RWMutex
}

// NewLocalMessenger creates a new local messenger that uses the bus package for message passing.
func NewLocalMessenger() Messenger {
	return &local{
		local:     bus.NewBus(),
		stopCh:    make(chan struct{}),
		listeners: make(map[string][]MessageListener),
	}
}

func (l *local) GracefulStop() {
	l.local.Close()
	if l.stopCh != nil {
		close(l.stopCh)
	}
}

func (l *local) Serve() run.StopNotify {
	return l.stopCh
}

func (l *local) Name() string {
	return "local-gossip-messenger"
}

func (l *local) Subscribe(topic string, listener MessageListener) error {
	l.listenersLock.Lock()
	defer l.listenersLock.Unlock()
	if _, ok := l.listeners[topic]; !ok {
		l.listeners[topic] = make([]MessageListener, 0)
	}
	l.listeners[topic] = append(l.listeners[topic], listener)
	return nil
}

func (l *local) Propagation(_ []string, _ string, _ bus.Message) (Future, error) {
	// no needs to propagate in local messenger
	return nil, fmt.Errorf("propagation not supported on local messenger")
}
