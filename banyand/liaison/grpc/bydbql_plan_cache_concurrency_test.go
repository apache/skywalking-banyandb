//go:build querymetrics

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

package grpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/apache/skywalking-banyandb/pkg/bydbql"
)

func TestCompiledPlanWaiterCancellation(t *testing.T) {
	for _, cancelAll := range []bool{false, true} {
		name := "first_waiter"
		if cancelAll {
			name = "all_waiters"
		}
		t.Run(name, func(t *testing.T) {
			cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20})
			query := "SELECT svc AS node FROM g.m LIMIT ?"
			statement, err := bydbql.Prepare(query)
			require.NoError(t, err)
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			restore := SetBydbQLPlanCacheHooksForTest(PlanCacheTestHooks{BeforeCompile: func(ctx context.Context, _ string) {
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-ctx.Done():
				}
			}})
			defer restore()
			firstCtx, cancelFirst := context.WithCancel(t.Context())
			defer cancelFirst()
			secondCtx, cancelSecond := context.WithCancel(t.Context())
			defer cancelSecond()
			var group errgroup.Group
			group.Go(func() error {
				handle, _, acquireErr := cache.acquire(firstCtx, query, statement)
				handle.Release()
				if !errors.Is(acquireErr, context.Canceled) {
					return errors.New("first waiter did not cancel independently")
				}
				return nil
			})
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("compile did not start")
			}
			group.Go(func() error {
				handle, _, acquireErr := cache.acquire(secondCtx, query, statement)
				handle.Release()
				if cancelAll {
					if !errors.Is(acquireErr, context.Canceled) {
						return errors.New("last waiter did not cancel")
					}
					return nil
				}
				return acquireErr
			})
			require.Eventually(t, func() bool { return cache.snapshot().Waiters == 2 }, 5*time.Second, time.Millisecond)
			cancelFirst()
			require.Eventually(t, func() bool { return cache.snapshot().Waiters == 1 }, 5*time.Second, time.Millisecond)
			if cancelAll {
				cancelSecond()
			} else {
				once.Do(func() { close(release) })
			}
			require.NoError(t, group.Wait())
			require.Eventually(t, func() bool { return cache.snapshot().InFlight == 0 }, 5*time.Second, time.Millisecond)
			require.Zero(t, cache.snapshot().CompileReservedBytes)
			if cancelAll {
				require.Zero(t, cache.snapshot().Entries)
				once.Do(func() { close(release) })
			}
			recovered, outcome, recoveryErr := cache.acquire(t.Context(), query, statement)
			require.NoError(t, recoveryErr)
			if cancelAll {
				require.Equal(t, "miss", outcome)
			} else {
				require.Equal(t, "hit", outcome)
			}
			recovered.Release()
		})
	}
}

func TestCompiledPlanCloseCancelsSharedWork(t *testing.T) {
	cache, _ := planCacheFixture(t, planCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20})
	query := "SELECT svc AS node FROM g.m LIMIT ?"
	statement, err := bydbql.Prepare(query)
	require.NoError(t, err)
	started := make(chan struct{}, 1)
	restore := SetBydbQLPlanCacheHooksForTest(PlanCacheTestHooks{BeforeCompile: func(ctx context.Context, _ string) {
		started <- struct{}{}
		<-ctx.Done()
	}})
	defer restore()
	var group errgroup.Group
	group.Go(func() error {
		handle, _, acquireErr := cache.acquire(t.Context(), query, statement)
		handle.Release()
		if !errors.Is(acquireErr, context.Canceled) {
			return errors.New("shutdown did not cancel compilation")
		}
		return nil
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("compile did not start")
	}
	cache.Close()
	require.NoError(t, group.Wait())
	stats := cache.snapshot()
	require.Zero(t, stats.InFlight)
	require.Zero(t, stats.Waiters)
	require.Zero(t, stats.RetainedBytes)
	require.Zero(t, stats.CompileReservedBytes)
	handle, _, err := cache.acquire(t.Context(), query, statement)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, handle)
}
