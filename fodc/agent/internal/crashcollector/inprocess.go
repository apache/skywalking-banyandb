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
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/pkg/panicdiag"
)

const defaultInProcessCapacity = 32

// InProcessPanicStore records panics from in-process goroutines into a bounded ring buffer
// so they appear alongside remote and file-based collections on the /diagnostics endpoint.
type InProcessPanicStore struct {
	records *flightrecorder.RingBuffer[CollectionRecord]
}

// NewInProcessPanicStore returns an InProcessPanicStore that retains up to capacity panic records.
func NewInProcessPanicStore(capacity int) *InProcessPanicStore {
	if capacity <= 0 {
		capacity = defaultInProcessCapacity
	}
	rb := flightrecorder.NewRingBuffer[CollectionRecord]()
	rb.SetCapacity(capacity)
	return &InProcessPanicStore{records: rb}
}

// Reporter returns a panicdiag.Reporter that records each recovered panic into this store.
// Register it as the process-wide default via panicdiag.SetDefaultReporter so all goroutines
// wrapped with GoWithRecovery(nil reporter) automatically route here.
func (s *InProcessPanicStore) Reporter() panicdiag.Reporter {
	return func(_ context.Context, result panicdiag.RecoveryResult) {
		artifactKey := result.ArtifactDir
		if artifactKey == "" {
			artifactKey = fmt.Sprintf("in-process-%s-%s",
				result.Record.Component,
				result.Record.OccurredAt.UTC().Format(time.RFC3339Nano))
		}
		s.records.Add(CollectionRecord{
			FetchedAt:      result.Record.OccurredAt,
			SourceEndpoint: "fodc-agent",
			Collection: panicdiag.Collection{
				ArtifactDir: artifactKey,
				Record:      result.Record,
			},
		})
		s.records.FinalizeLastVisible()
	}
}

// ListCollections implements CollectionLister, returning all stored in-process panic records.
func (s *InProcessPanicStore) ListCollections() []CollectionRecord {
	if s == nil {
		return nil
	}
	return s.records.GetAllValues()
}
