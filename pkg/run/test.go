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

package run

import (
	"fmt"
	"sync"
)

var _ Service = (*tester)(nil)

type tester struct {
	startedNotifier chan struct{}
	stopCh          chan struct{}
	ID              string
	once            sync.Once
}

// NewTester return a tester module to stop the runtime programmatically.
func NewTester(id string) (Unit, func()) {
	t := &tester{
		ID:              id,
		startedNotifier: make(chan struct{}),
		stopCh:          make(chan struct{}),
		once:            sync.Once{},
	}
	return t, t.GracefulStop
}

func (t *tester) WaitUntilStarted() error {
	select {
	case err := <-t.stopCh:
		return fmt.Errorf("stopped: %v", err)
	case <-t.startedNotifier:
		return nil
	}
}

func (t *tester) Name() string {
	return t.ID
}

func (t *tester) Serve() StopNotify {
	close(t.startedNotifier)
	return t.stopCh
}

func (t *tester) GracefulStop() {
	t.once.Do(func() {
		close(t.stopCh)
	})
}
