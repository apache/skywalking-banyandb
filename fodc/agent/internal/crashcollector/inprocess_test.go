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

package crashcollector

import (
	"context"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

func TestInProcessPanicStoreRecordsPanic(t *testing.T) {
	t.Helper()

	store := NewInProcessPanicStore(8)
	reporter := store.Reporter()

	occurredAt := time.Date(2026, time.April, 20, 10, 0, 0, 0, time.UTC)
	reporter(context.Background(), panicdiag.RecoveryResult{
		Record: &panicdiag.PanicRecord{
			OccurredAt: occurredAt,
			Component:  "fodc-agent-heartbeat",
			PanicValue: "nil pointer dereference",
			Recovered:  true,
		},
		ArtifactDir: "",
	})

	records := store.ListCollections()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]
	if r.SourceEndpoint != "fodc-agent" {
		t.Fatalf("unexpected source endpoint: %s", r.SourceEndpoint)
	}
	if r.Collection.Record == nil {
		t.Fatal("expected non-nil PanicRecord in Collection")
	}
	if r.Collection.Record.Component != "fodc-agent-heartbeat" {
		t.Fatalf("unexpected component: %s", r.Collection.Record.Component)
	}
	if r.Collection.ArtifactDir == "" {
		t.Fatal("expected synthetic artifact key when no artifact dir is set")
	}
}

func TestInProcessPanicStorePreservesArtifactDir(t *testing.T) {
	t.Helper()

	store := NewInProcessPanicStore(8)
	reporter := store.Reporter()

	reporter(context.Background(), panicdiag.RecoveryResult{
		Record: &panicdiag.PanicRecord{
			OccurredAt: time.Now().UTC(),
			Component:  "fodc-agent-ktm-bridge",
			PanicValue: "index out of range",
			Recovered:  true,
		},
		ArtifactDir: "20260420T100000Z-fodc-agent-ktm-bridge-42",
	})

	records := store.ListCollections()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Collection.ArtifactDir != "20260420T100000Z-fodc-agent-ktm-bridge-42" {
		t.Fatalf("artifact dir not preserved: %s", records[0].Collection.ArtifactDir)
	}
}

func TestInProcessPanicStoreCapacity(t *testing.T) {
	t.Helper()

	store := NewInProcessPanicStore(3)
	reporter := store.Reporter()
	now := time.Now().UTC()

	for i := range 5 {
		reporter(context.Background(), panicdiag.RecoveryResult{
			Record: &panicdiag.PanicRecord{
				OccurredAt: now.Add(time.Duration(i) * time.Second),
				Component:  "fodc-agent-heartbeat",
				PanicValue: "boom",
				Recovered:  true,
			},
		})
	}

	records := store.ListCollections()
	if len(records) > 3 {
		t.Fatalf("expected at most 3 records (ring buffer capacity), got %d", len(records))
	}
}

func TestInProcessPanicStoreIntegratesWithMultiProvider(t *testing.T) {
	t.Helper()

	store := NewInProcessPanicStore(8)
	reporter := store.Reporter()

	reporter(context.Background(), panicdiag.RecoveryResult{
		Record: &panicdiag.PanicRecord{
			OccurredAt: time.Now().UTC(),
			Component:  "crash-collector",
			PanicValue: "runtime error",
			Recovered:  true,
		},
	})

	multi := NewMultiCollectionProvider(store)
	all := multi.ListCollections()
	if len(all) != 1 {
		t.Fatalf("expected 1 record from multi-provider, got %d", len(all))
	}

	data, marshalErr := multi.MarshalCollections()
	if marshalErr != nil {
		t.Fatalf("marshal failed: %v", marshalErr)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON")
	}
}
