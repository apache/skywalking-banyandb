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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type localDelivery struct {
	client  Client
	topic   bus.Topic
	timeout time.Duration
}

// NewLocalDelivery returns the Delivery that publishes each batch to topic
// through client's batch publisher, one message per request. A topic with no
// subscriber yet is a retryable failure: on the local pipeline it is a service
// that has not started.
func NewLocalDelivery(client Client, topic bus.Topic, timeout time.Duration) Delivery {
	return &localDelivery{client: client, topic: topic, timeout: timeout}
}

func (l *localDelivery) Deliver(ctx context.Context, batch []any) error {
	messages := make([]bus.Message, 0, len(batch))
	for _, data := range batch {
		messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), "", data))
	}
	publisher := l.client.NewBatchPublisher(l.timeout)
	pubCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	_, err := publisher.Publish(pubCtx, l.topic, messages...)
	nodeErrs, closeErr := publisher.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		// Close reports a destination's refusal in its map and returns a nil
		// error alongside it. On the local pipeline that map is the only report
		// there is: Publish buffers and cannot fail, and the bus drops the
		// payload of an unhealthy listener while returning the reason here. So
		// discarding it means a refused batch is counted as delivered.
		err = rejection(nodeErrs)
	}
	if errors.Is(err, bus.ErrTopicNotExist) {
		return Retryable(err)
	}
	return err
}

// rejection turns a publisher's per-destination error map into one error. Any
// entry means the batch did not land: a local batch has a single destination,
// so there is no partial success to preserve.
func rejection(nodeErrs map[string]*common.Error) error {
	for node, ce := range nodeErrs {
		if ce == nil {
			continue
		}
		if node == "" {
			node = "the destination"
		}
		return fmt.Errorf("%s refused the batch: %w", node, ce)
	}
	return nil
}
