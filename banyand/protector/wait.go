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

package protector

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	waitInitialBackoff = 200 * time.Millisecond
	waitMaxBackoff     = 2 * time.Second
	waitHeartbeat      = time.Minute
)

// WaitWhileHigh blocks while memory pressure is high, bounded by maxWait.
// It only creates a timeout context when it actually has to wait. It returns
// the timeout/cancel error if ctx or maxWait elapses before recovery.
func WaitWhileHigh(ctx context.Context, pm Memory, maxWait time.Duration, l *logger.Logger, stage string) error {
	if pm.State() != StateHigh {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	start := time.Now()
	l.Warn().Str("stage", stage).Dur("max_wait", maxWait).Msg("memory high, migration receive paused, waiting")
	ticker := time.NewTicker(waitHeartbeat)
	defer ticker.Stop()
	backoff := waitInitialBackoff
	for pm.State() == StateHigh {
		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > waitMaxBackoff {
				backoff = waitMaxBackoff
			}
		case <-ticker.C:
			l.Warn().Str("stage", stage).Dur("waited", time.Since(start)).Msg("still waiting for memory")
		case <-waitCtx.Done():
			l.Warn().Str("stage", stage).Dur("waited", time.Since(start)).Msg("memory wait aborted (timeout/cancel)")
			return waitCtx.Err()
		}
	}
	l.Info().Str("stage", stage).Dur("waited", time.Since(start)).Msg("memory recovered, migration receive resumed")
	return nil
}
