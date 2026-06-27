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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
)

// defaultLifecycleSendRetryTimeout bounds the total wall-clock spent retrying
// transient failures when streaming a part to a target node (e.g. while the
// target node restarts). Configurable via --lifecycle-send-retry-timeout.
const defaultLifecycleSendRetryTimeout = 15 * time.Minute

// lifecycleSendRetryTimeout is the active bound, set by the lifecycle flag.
var lifecycleSendRetryTimeout = defaultLifecycleSendRetryTimeout

// isNoNodes reports whether err means the target node could not be picked
// because no node is currently available (e.g. the target is restarting).
// It matches both the typed sentinel and the plain round-robin error string.
func isNoNodes(err error) bool {
	if errors.Is(err, node.ErrNoAvailableNode) {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "no nodes available")
}

// isTransientSend reports whether err from streaming a part to a node is
// transient and worth retrying: receiver back-pressure (SERVER_BUSY) or a
// transient/failover gRPC error such as Unavailable, ResourceExhausted, or a
// dropped connection.
func isTransientSend(err error) bool {
	return errors.Is(err, queue.ErrServerBusy) || grpchelper.IsTransientError(err) || grpchelper.IsFailoverError(err)
}

// retryLogInterval throttles the "still retrying" heartbeat so a long wait for
// a restarting node logs at most once per minute instead of on every attempt.
const retryLogInterval = time.Minute

// nodePicker is the one method of node.Selector that the send-retry path needs;
// narrowing it keeps runWithRetry/pickWithRetry decoupled and easy to test.
type nodePicker interface {
	Pick(group, name string, shardID, replicaID uint32) (string, error)
}

// runWithRetry runs op under bounded exponential backoff capped by
// lifecycleSendRetryTimeout. op returns a transient error to retry or
// backoff.Permanent(err) to stop immediately. While retrying it logs a
// heartbeat at most once per minute so a long wait for a restarting node is not
// mistaken for a hang. ctx cancellation aborts the retry promptly (returning
// ctx.Err()) instead of waiting out the timeout.
func runWithRetry(ctx context.Context, l *logger.Logger, stage string, op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = lifecycleSendRetryTimeout
	start := time.Now()
	var lastLog time.Time
	return backoff.RetryNotify(op, backoff.WithContext(bo, ctx), func(err error, _ time.Duration) {
		now := time.Now()
		if !lastLog.IsZero() && now.Sub(lastLog) < retryLogInterval {
			return
		}
		lastLog = now
		l.Warn().Err(err).Str("stage", stage).Dur("elapsed", now.Sub(start)).
			Msg("transient lifecycle send failure, still retrying")
	})
}

// pickWithRetry picks a target node, retrying on "no nodes available" (target
// restarting) under runWithRetry's bounded backoff and heartbeat logging.
func pickWithRetry(ctx context.Context, l *logger.Logger, p nodePicker, group, name string, shardID uint32) (string, error) {
	var nodeID string
	err := runWithRetry(ctx, l, fmt.Sprintf("pick group=%s name=%s shard=%d", group, name, shardID), func() error {
		var pErr error
		nodeID, pErr = p.Pick(group, name, shardID, 0)
		switch {
		case pErr == nil:
			return nil
		case isNoNodes(pErr):
			return pErr
		default:
			return backoff.Permanent(pErr)
		}
	})
	return nodeID, err
}

// pickAndRun picks a target node and runs run against it inside one bounded
// backoff loop: a "no nodes" pick error or a transient run error retries the
// whole pick+run (so a failed send re-picks, possibly a healthier node); any
// other error stops immediately. Heartbeat-logged like runWithRetry.
func pickAndRun(ctx context.Context, l *logger.Logger, p nodePicker, group, name string, shardID, replicaID uint32,
	run func(nodeID string) error,
) error {
	return runWithRetry(ctx, l, fmt.Sprintf("send group=%s name=%s shard=%d replica=%d", group, name, shardID, replicaID), func() error {
		nodeID, pickErr := p.Pick(group, name, shardID, replicaID)
		if pickErr != nil {
			if isNoNodes(pickErr) {
				return pickErr
			}
			return backoff.Permanent(pickErr)
		}
		if runErr := run(nodeID); runErr != nil {
			if isTransientSend(runErr) {
				return runErr
			}
			return backoff.Permanent(runErr)
		}
		return nil
	})
}
