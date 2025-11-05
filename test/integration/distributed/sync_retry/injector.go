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

package integration_sync_retry_test

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/queue"
)

type topicFailureRule struct {
	remaining int
	attempts  int
}

type chunkedSyncTestInjector struct {
	rules map[string]*topicFailureRule
	mu    sync.Mutex
}

func newChunkedSyncTestInjector(config map[string]int) *chunkedSyncTestInjector {
	rules := make(map[string]*topicFailureRule, len(config))
	for topic, remaining := range config {
		rules[topic] = &topicFailureRule{remaining: remaining}
	}
	return &chunkedSyncTestInjector{rules: rules}
}

func (c *chunkedSyncTestInjector) BeforeSync(parts []queue.StreamingPartData) (bool, []queue.FailedPart, error) {
	if len(parts) == 0 {
		return false, nil, nil
	}

	topic := parts[0].Topic

	c.mu.Lock()
	defer c.mu.Unlock()

	rule, ok := c.rules[topic]
	if !ok {
		return false, nil, nil
	}

	rule.attempts++
	if rule.remaining == 0 {
		return false, nil, nil
	}

	if rule.remaining > 0 {
		rule.remaining--
	}

	failedParts := make([]queue.FailedPart, 0, len(parts))
	for _, part := range parts {
		failedParts = append(failedParts, queue.FailedPart{
			PartID: strconv.FormatUint(part.ID, 10),
			Error:  fmt.Sprintf("injected failure (topic=%s attempt=%d)", topic, rule.attempts),
		})
	}

	return true, failedParts, nil
}

func (c *chunkedSyncTestInjector) attemptsFor(topic string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rule, ok := c.rules[topic]; ok {
		return rule.attempts
	}
	return 0
}

func withChunkedSyncFailureInjector(config map[string]int) (*chunkedSyncTestInjector, func()) {
	injector := newChunkedSyncTestInjector(config)
	queue.RegisterChunkedSyncFailureInjector(injector)
	cleanup := func() {
		queue.ClearChunkedSyncFailureInjector()
	}
	return injector, cleanup
}
