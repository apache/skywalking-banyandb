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

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type remoteDelivery struct {
	client  Client
	locate  func() (string, error)
	topic   bus.Topic
	timeout time.Duration
}

// NewRemoteDelivery returns the Delivery that publishes each batch to the node
// locate returns, through client's batch publisher, one message per request.
// locate runs on every attempt, so a retried batch goes to the node it returns
// then.
//
// What never reached a node is retryable: locate failed, or every message
// failed before it was sent. What a node answered with an error, or did not
// answer in time, is not: the node decided, or may have stored the batch, and
// a retry could store it twice.
func NewRemoteDelivery(client Client, topic bus.Topic, locate func() (string, error), timeout time.Duration) Delivery {
	return &remoteDelivery{client: client, topic: topic, locate: locate, timeout: timeout}
}

func (r *remoteDelivery) Deliver(ctx context.Context, batch []any) error {
	node, err := r.locate()
	if err != nil {
		return Retryable(err)
	}
	if node == "" {
		return Retryable(errors.New("no node located for the batch"))
	}
	messages := make([]bus.Message, 0, len(batch))
	for _, data := range batch {
		messages = append(messages, bus.NewBatchMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, data))
	}
	publisher := r.client.NewBatchPublisher(r.timeout)
	pubCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	_, pubErr := publisher.Publish(pubCtx, r.topic, messages...)
	nodeErrs, closeErr := publisher.Close()
	if err = rejection(nodeErrs); err != nil {
		return err
	}
	if pubErr != nil {
		if noneSent(pubErr, len(messages)) {
			return Retryable(pubErr)
		}
		return pubErr
	}
	if closeErr != nil {
		return closeErr
	}
	if pubCtx.Err() != nil {
		// The publisher ends a stream that outlives its timeout without an
		// error of its own, so the deadline is the only report there is.
		return fmt.Errorf("%s did not answer within %s: %w", node, r.timeout, pubCtx.Err())
	}
	return nil
}

// noneSent reports whether err holds one error for each of n messages, every
// one raised before its message was sent. A message that was sent has no
// error, and one that failed while sending has an error of another kind.
func noneSent(err error, n int) bool {
	errs := multierr.Errors(err)
	if len(errs) != n {
		return false
	}
	for _, e := range errs {
		if !errors.Is(e, ErrNotSent) {
			return false
		}
	}
	return true
}
