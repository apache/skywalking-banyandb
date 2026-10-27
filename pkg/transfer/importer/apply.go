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

package importer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// barrierKinds maps a schemasync kind to the kind name of a SchemaBarrierService key.
var barrierKinds = map[string]string{
	schemasync.KindGroup:            "group",
	schemasync.KindStream:           "stream",
	schemasync.KindMeasure:          "measure",
	schemasync.KindTrace:            "trace",
	schemasync.KindProperty:         "property",
	schemasync.KindIndexRule:        "index_rule",
	schemasync.KindIndexRuleBinding: "index_rule_binding",
	schemasync.KindTopNAggregation:  "top_n_aggregation",
}

// WriteSchema creates or updates every resource of writes, in their (dependency) order, and
// returns the barrier request that waits for all of them, at the revision the registry
// answered, plus the _top_n_result of every measure group it creates when the artifact has
// one in that group (the target creates it asynchronously). A resource created by someone
// else since the comparison makes Create fail with ALREADY_EXISTS; that and every other
// failure is a runtime error (exit 3): earlier resources stay written.
func WriteSchema(ctx context.Context, conn *grpc.ClientConn, writes []SchemaAction, art *Schemas) (*schemav1.AwaitSchemaAppliedRequest, error) {
	req := &schemav1.AwaitSchemaAppliedRequest{}
	for i := range writes {
		w := &writes[i]
		create := w.Disposition == DispositionCreate
		rev, err := writeOne(ctx, conn, w)
		if err != nil {
			verb := "update"
			if create {
				verb = "create"
			}
			hint := ""
			if status.Code(err) == codes.AlreadyExists {
				hint = " (created by someone else since the comparison; rerun the import to compare again)"
			}
			return nil, exitcode.Exit(exitcode.ExitRuntime, "%s %s%s: %w; %d of %d schema resource(s) were written before it",
				verb, w.ID(), hint, err, i, len(writes))
		}
		if key := barrierKey(w); key != nil {
			req.Keys = append(req.Keys, key)
			req.MinRevisions = append(req.MinRevisions, rev)
		}
		if g, ok := w.Spec.(*commonv1.Group); ok && create && g.GetCatalog() == commonv1.Catalog_CATALOG_MEASURE {
			if _, has := art.Measures[ResourceKey{Group: w.Group, Name: csvfmt.TopNResultMeasure}]; has {
				req.Keys = append(req.Keys, &schemav1.SchemaKey{Kind: "measure", Group: w.Group, Name: csvfmt.TopNResultMeasure})
				req.MinRevisions = append(req.MinRevisions, 0)
			}
		}
	}
	return req, nil
}

// barrierKey is the barrier key of a written resource; a group is named by its name. It is nil
// for a property group, which no repository of a data node keeps, so the barrier would hold
// the wait until it times out; the property schemas of the group are awaited instead.
func barrierKey(w *SchemaAction) *schemav1.SchemaKey {
	if g, ok := w.Spec.(*commonv1.Group); ok && g.GetCatalog() == commonv1.Catalog_CATALOG_PROPERTY {
		return nil
	}
	if w.Kind == schemasync.KindGroup {
		return &schemav1.SchemaKey{Kind: barrierKinds[w.Kind], Name: w.Group}
	}
	return &schemav1.SchemaKey{Kind: barrierKinds[w.Kind], Group: w.Group, Name: w.Name}
}

// writeOne creates or updates one resource and returns the registry's mod revision.
func writeOne(ctx context.Context, conn *grpc.ClientConn, w *SchemaAction) (int64, error) {
	create := w.Disposition == DispositionCreate
	switch spec := w.Spec.(type) {
	case *commonv1.Group:
		c := databasev1.NewGroupRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.GroupRegistryServiceUpdateRequest{Group: spec}))
	case *databasev1.Stream:
		c := databasev1.NewStreamRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{Stream: spec}))
	case *databasev1.Measure:
		c := databasev1.NewMeasureRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{Measure: spec}))
	case *databasev1.Trace:
		c := databasev1.NewTraceRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{Trace: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.TraceRegistryServiceUpdateRequest{Trace: spec}))
	case *databasev1.Property:
		c := databasev1.NewPropertyRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.PropertyRegistryServiceCreateRequest{Property: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.PropertyRegistryServiceUpdateRequest{Property: spec}))
	case *databasev1.IndexRule:
		c := databasev1.NewIndexRuleRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.IndexRuleRegistryServiceUpdateRequest{IndexRule: spec}))
	case *databasev1.IndexRuleBinding:
		c := databasev1.NewIndexRuleBindingRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{IndexRuleBinding: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{IndexRuleBinding: spec}))
	case *databasev1.TopNAggregation:
		c := databasev1.NewTopNAggregationRegistryServiceClient(conn)
		if create {
			return modRevision(c.Create(ctx, &databasev1.TopNAggregationRegistryServiceCreateRequest{TopNAggregation: spec}))
		}
		return modRevision(c.Update(ctx, &databasev1.TopNAggregationRegistryServiceUpdateRequest{TopNAggregation: spec}))
	default:
		return 0, fmt.Errorf("unsupported schema resource %T", w.Spec)
	}
}

// modRevision returns the mod revision of a registry response; protobuf getters are nil-safe.
func modRevision[R interface{ GetModRevision() int64 }](resp R, err error) (int64, error) {
	return resp.GetModRevision(), err
}

// AwaitSchema blocks until every key of req is applied on every node (SchemaBarrierService),
// or timeout elapses: a timeout or failure is a runtime error (exit 3) naming the laggards.
func AwaitSchema(ctx context.Context, conn *grpc.ClientConn, req *schemav1.AwaitSchemaAppliedRequest, timeout time.Duration) error {
	if len(req.GetKeys()) == 0 {
		return nil
	}
	req.Timeout = durationpb.New(timeout)
	callCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()
	resp, err := schemav1.NewSchemaBarrierServiceClient(conn).AwaitSchemaApplied(callCtx, req)
	if err != nil {
		return exitcode.Exit(exitcode.ExitRuntime, "wait for the written schema to be applied: %w", err)
	}
	if resp.GetApplied() {
		return nil
	}
	var lag []string
	for _, l := range resp.GetLaggards() {
		var keys []string
		for _, k := range l.GetMissingKeys() {
			keys = append(keys, k.GetKind()+"/"+k.GetGroup()+"/"+k.GetName())
		}
		s := l.GetNode() + " missing [" + strings.Join(keys, " ") + "]"
		if l.GetReason() != "" {
			s += " (" + l.GetReason() + ")"
		}
		lag = append(lag, s)
	}
	return exitcode.Exit(exitcode.ExitRuntime, "the written schema was not applied on every node within %s: %s; the schema is written, no data was sent",
		timeout, strings.Join(lag, "; "))
}
