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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

func TestRepairStopsWaitingForTheStripeWhenCanceled(t *testing.T) {
	gate := newBlockGate()
	sd := newRealMemoryShard(t)
	sd.testBeforeNativeRead = gate.hook
	newProperty := func(revision int64) *propertyv1.Property {
		return &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: "g", Name: "n", ModRevision: revision},
			Id:       "entity",
		}
	}

	// The first repair takes the entity's stripe and then blocks in its leading read.
	holder := newProperty(1)
	holderDone := make(chan error, 1)
	go func() {
		_, _, err := sd.repair(context.Background(), GetPropertyID(holder), holder, 0)
		holderDone <- err
	}()
	<-gate.entered

	ctx, cancel := context.WithCancel(context.Background())
	waiter := newProperty(2)
	waiterDone := make(chan error, 1)
	go func() {
		_, _, err := sd.repair(ctx, GetPropertyID(waiter), waiter, 0)
		waiterDone <- err
	}()
	select {
	case err := <-waiterDone:
		t.Fatalf("a repair of the same entity did not wait for the held stripe: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled repair kept waiting for the held stripe")
	}

	close(gate.release)
	require.NoError(t, <-holderDone)
}

func TestRepairStripeRefusesAnEndedContext(t *testing.T) {
	var stripe repairStripe
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, stripe.lock(ctx), context.Canceled)
	require.NoError(t, stripe.lock(context.Background()), "a refused acquisition must leave the stripe free")
	stripe.unlock()
}
