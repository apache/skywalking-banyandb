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

// ErrNoNodeLocated is the delivery failure of a batch that no node was located
// for: the locate failed, or it returned no node. Its cause, if any, is
// wrapped with it.
var ErrNoNodeLocated = errors.New("no node located for the batch")

type localDelivery struct {
	client Client
	// node is nil on the local pipeline, which has a single destination.
	node    func() (string, error)
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

// NewNodeDelivery returns the same Delivery for a client that reaches other
// nodes: each batch goes to the node that node returns when the batch is
// delivered. A failed locate is retryable, because nothing was published;
// once a batch is published, its outcome is final.
func NewNodeDelivery(client Client, topic bus.Topic, node func() (string, error), timeout time.Duration) Delivery {
	return &localDelivery{client: client, topic: topic, node: node, timeout: timeout}
}

func (l *localDelivery) Deliver(ctx context.Context, batch []any) error {
	var target string
	if l.node != nil {
		located, err := l.node()
		if err != nil {
			return Retryable(fmt.Errorf("%w: %w", ErrNoNodeLocated, err))
		}
		if located == "" {
			return Retryable(ErrNoNodeLocated)
		}
		target = located
	}
	messages := make([]bus.Message, 0, len(batch))
	for _, data := range batch {
		messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), target, data))
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
	if err == nil && l.node != nil && pubCtx.Err() != nil {
		// A remote publisher ends a stream that outlives its timeout without
		// an error of its own, so the deadline is the only report there is.
		// The local pipeline does not honor it, so there it reports nothing.
		err = fmt.Errorf("%s did not answer within %s: %w", target, l.timeout, pubCtx.Err())
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
