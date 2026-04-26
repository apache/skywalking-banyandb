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

package grpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

const (
	barrierMaxKeys      = 10000
	barrierInitInterval = 10 * time.Millisecond
	barrierMaxInterval  = 500 * time.Millisecond
	barrierGrowthFactor = 1.5
	// barrierDefaultTimeout bounds calls that pass a nil/zero timeout. It must
	// be large enough to cover normal schema propagation (watch event delivery
	// + handler dispatch + tsdb open) so a no-timeout caller does not see false
	// applied=false. Distinct from barrierMaxInterval, which only caps the poll
	// backoff between probes.
	barrierDefaultTimeout = 5 * time.Second
)

// barrierCacheReader abstracts the property schema cache so the barrier service
// can poll for mod-revision progress without coupling to a concrete type.
type barrierCacheReader interface {
	GetMaxModRevision() int64
	GetKeyModRevision(propID string) (int64, bool)
}

type barrierService struct {
	schemav1.UnimplementedSchemaBarrierServiceServer
	cacheProvider func() barrierCacheReader
}

func newBarrierService(cacheProvider func() barrierCacheReader) *barrierService {
	return &barrierService{cacheProvider: cacheProvider}
}

// cache resolves the current barrier cache reader. It returns nil if the
// underlying schema registry is not yet initialized.
func (b *barrierService) cache() barrierCacheReader {
	if b.cacheProvider == nil {
		return nil
	}
	return b.cacheProvider()
}

// AwaitRevisionApplied blocks until the cache's max modRevision is >= req.MinRevision
// or the timeout elapses. In standalone mode there is one node, so Laggards carries a
// single entry whose current_mod_revision reports the cache watermark — this lets
// callers diagnose how far behind the standalone cache is even when applied=false.
func (b *barrierService) AwaitRevisionApplied(ctx context.Context, req *schemav1.AwaitRevisionAppliedRequest) (*schemav1.AwaitRevisionAppliedResponse, error) {
	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	interval := barrierInitInterval
	currentRevision := func() int64 {
		if c := b.cache(); c != nil {
			return c.GetMaxModRevision()
		}
		return 0
	}
	for {
		if currentRevision() >= req.GetMinRevision() {
			return &schemav1.AwaitRevisionAppliedResponse{Applied: true}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{CurrentModRevision: currentRevision()}},
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitRevisionAppliedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{CurrentModRevision: currentRevision()}},
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// AwaitSchemaApplied blocks until all requested keys are present at or above their
// per-key min_revisions, or the timeout elapses.
func (b *barrierService) AwaitSchemaApplied(ctx context.Context, req *schemav1.AwaitSchemaAppliedRequest) (*schemav1.AwaitSchemaAppliedResponse, error) {
	if len(req.GetKeys()) > barrierMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", barrierMaxKeys)
	}
	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	interval := barrierInitInterval
	for {
		missing := b.collectMissingKeys(req.GetKeys(), req.GetMinRevisions())
		if len(missing) == 0 {
			return &schemav1.AwaitSchemaAppliedResponse{Applied: true}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitSchemaAppliedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{MissingKeys: missing}},
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitSchemaAppliedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{MissingKeys: missing}},
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// AwaitSchemaDeleted blocks until all requested keys are absent from the cache,
// or the timeout elapses.
func (b *barrierService) AwaitSchemaDeleted(ctx context.Context, req *schemav1.AwaitSchemaDeletedRequest) (*schemav1.AwaitSchemaDeletedResponse, error) {
	if len(req.GetKeys()) > barrierMaxKeys {
		return nil, status.Errorf(codes.InvalidArgument, "too many keys: max=%d", barrierMaxKeys)
	}
	deadline := time.Now().Add(barrierDeadlineDuration(req.GetTimeout()))
	pollCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	interval := barrierInitInterval
	for {
		stillPresent := b.collectPresentKeys(req.GetKeys())
		if len(stillPresent) == 0 {
			return &schemav1.AwaitSchemaDeletedResponse{Applied: true}, nil
		}
		if time.Now().After(deadline) {
			return &schemav1.AwaitSchemaDeletedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{StillPresentKeys: stillPresent}},
			}, nil
		}
		select {
		case <-time.After(interval):
		case <-pollCtx.Done():
			return &schemav1.AwaitSchemaDeletedResponse{
				Applied:  false,
				Laggards: []*schemav1.NodeLaggard{{StillPresentKeys: stillPresent}},
			}, nil
		}
		interval = barrierBackoff(interval)
	}
}

// collectMissingKeys returns the subset of keys that are not yet present in the
// cache at or above their required mod_revision. When the cache is not yet
// initialized, all keys are reported as missing so polling continues.
func (b *barrierService) collectMissingKeys(keys []*schemav1.SchemaKey, minRevisions []int64) []*schemav1.SchemaKey {
	c := b.cache()
	if c == nil {
		missing := make([]*schemav1.SchemaKey, len(keys))
		copy(missing, keys)
		return missing
	}
	var missing []*schemav1.SchemaKey
	for idx, key := range keys {
		propID := schemaKeyToPropID(key)
		rev, ok := c.GetKeyModRevision(propID)
		var minRev int64
		if idx < len(minRevisions) {
			minRev = minRevisions[idx]
		}
		if !ok || (minRev > 0 && rev < minRev) {
			missing = append(missing, key)
		}
	}
	return missing
}

// collectPresentKeys returns the subset of keys that are still present in the cache.
// When the cache is not yet initialized, all keys are reported as still-present
// to avoid falsely claiming successful deletion before the cache is online.
func (b *barrierService) collectPresentKeys(keys []*schemav1.SchemaKey) []*schemav1.SchemaKey {
	c := b.cache()
	if c == nil {
		present := make([]*schemav1.SchemaKey, len(keys))
		copy(present, keys)
		return present
	}
	var present []*schemav1.SchemaKey
	for _, key := range keys {
		propID := schemaKeyToPropID(key)
		if _, ok := c.GetKeyModRevision(propID); ok {
			present = append(present, key)
		}
	}
	return present
}

// schemaKeyToPropID converts a SchemaKey to the property-ID string format used
// by the schema cache. The propID format mirrors property.BuildPropertyID:
// kindString + "_" + group + "/" + name (or kindString + "_" + name for groups).
// SchemaKey.Kind uses proto underscore_case; kind.String() uses camelCase for compound
// kinds, so this function normalises the mapping.
func schemaKeyToPropID(key *schemav1.SchemaKey) string {
	kindStr := protoKindToString(key.GetKind())
	if key.GetKind() == "group" {
		return kindStr + "_" + key.GetName()
	}
	return kindStr + "_" + key.GetGroup() + "/" + key.GetName()
}

// protoKindToString maps the proto-style kind string from SchemaKey (underscore_case) to
// the value returned by schema.Kind.String() (camelCase for compound kinds).
func protoKindToString(protoKind string) string {
	switch protoKind {
	case "index_rule":
		return schema.KindIndexRule.String()
	case "index_rule_binding":
		return schema.KindIndexRuleBinding.String()
	case "top_n_aggregation":
		return schema.KindTopNAggregation.String()
	default:
		// For "stream", "measure", "trace", "group", "property", "node" the
		// proto kind string already matches kind.String().
		return protoKind
	}
}

// barrierBackoff returns the next poll interval, capped at barrierMaxInterval.
func barrierBackoff(d time.Duration) time.Duration {
	next := time.Duration(float64(d) * barrierGrowthFactor)
	if next > barrierMaxInterval {
		return barrierMaxInterval
	}
	return next
}

// barrierDeadlineDuration returns the effective wait duration from a proto Duration.
// A nil or zero timeout maps to barrierDefaultTimeout so callers that omit the field
// get a realistic propagation budget rather than the poll-interval cap.
func barrierDeadlineDuration(d *durationpb.Duration) time.Duration {
	if d == nil {
		return barrierDefaultTimeout
	}
	dur := d.AsDuration()
	if dur <= 0 {
		return barrierDefaultTimeout
	}
	return dur
}
