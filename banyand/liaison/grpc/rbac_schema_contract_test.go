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

package grpc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// The canonical fixture of the #13994 design: an operator administrator, a group-scoped
// tenant reader and writer, a wildcard reader that also proves future-group semantics, and an
// authenticated principal with no binding at all.
const schemaPolicyYAML = `
users:
  - username: "sch-admin"
    password: "admin-secret"
  - username: "sch-writer-alpha"
    password: "writer-alpha-secret"
  - username: "sch-reader-alpha"
    password: "reader-alpha-secret"
  - username: "sch-reader-all"
    password: "reader-all-secret"
  - username: "sch-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  bindings:
    - principal: "sch-admin"
      role: "admin"
      groups: ["*"]
    - principal: "sch-writer-alpha"
      role: "writer"
      groups: ["rbac-alpha"]
    - principal: "sch-reader-alpha"
      role: "reader"
      groups: ["rbac-alpha"]
    - principal: "sch-reader-all"
      role: "reader"
      groups: ["*"]
`

// widenedSchemaPolicyYAML is the same fixture with the alpha reader promoted to the wildcard
// scope. Compiling it as a second revision is how a test observes a policy change without
// touching the first revision, which callers holding it must keep seeing.
const widenedSchemaPolicyYAML = `
users:
  - username: "sch-admin"
    password: "admin-secret"
  - username: "sch-reader-alpha"
    password: "reader-alpha-secret"
rbac:
  enabled: true
  bindings:
    - principal: "sch-admin"
      role: "admin"
      groups: ["*"]
    - principal: "sch-reader-alpha"
      role: "reader"
      groups: ["*"]
`

// The two fixture groups. Data and schema names are duplicated across them so a scope leak
// is observable as a name that resolves in the wrong group rather than as a missing resource.
const (
	groupAlpha = "rbac-alpha"
	groupBeta  = "rbac-beta"
	groupGamma = "rbac-gamma"
	// fixtureMeasure is the child-schema name both fixture groups carry.
	fixtureMeasure = "service_cpm"

	streamRegistryService           = "StreamRegistryService"
	measureRegistryService          = "MeasureRegistryService"
	traceRegistryService            = "TraceRegistryService"
	indexRuleRegistryService        = "IndexRuleRegistryService"
	indexRuleBindingRegistryService = "IndexRuleBindingRegistryService"
	topNAggregationRegistryService  = "TopNAggregationRegistryService"
	propertyRegistryService         = "PropertyRegistryService"
)

// Schema method names, transcribed from the generated service descriptors' _FullMethodName
// constants. They are produced by protoc from the .proto files, so they are an independent
// source from anything this milestone adds.
const (
	mGroupGet           = "/banyandb.database.v1.GroupRegistryService/Get"
	mGroupExist         = "/banyandb.database.v1.GroupRegistryService/Exist"
	mGroupUpdate        = "/banyandb.database.v1.GroupRegistryService/Update"
	mGroupDelete        = "/banyandb.database.v1.GroupRegistryService/Delete"
	mAwaitSchemaApplied = "/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied"
	mAwaitSchemaDeleted = "/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaDeleted"
	mMeasureSchemaGet   = "/banyandb.database.v1.MeasureRegistryService/Get"
	mMeasureSchemaList  = "/banyandb.database.v1.MeasureRegistryService/List"
	mMeasureSchemaExist = "/banyandb.database.v1.MeasureRegistryService/Exist"
	mMeasureSchemaNew   = "/banyandb.database.v1.MeasureRegistryService/Create"
	mMeasureSchemaSet   = "/banyandb.database.v1.MeasureRegistryService/Update"
	mMeasureSchemaDrop  = "/banyandb.database.v1.MeasureRegistryService/Delete"
)

// registryServices is the fixed seven-family oracle of the issue: the registry services whose
// six-method rule family this milestone activates.
var registryServices = []string{
	streamRegistryService, measureRegistryService, traceRegistryService, indexRuleRegistryService,
	indexRuleBindingRegistryService, topNAggregationRegistryService, propertyRegistryService,
}

func registryMethod(service, method string) string {
	return "/banyandb.database.v1." + service + "/" + method
}

func schemaSnapshot(t *testing.T) auth.Snapshot {
	t.Helper()
	snap, err := auth.CompileSnapshot(1, []byte(schemaPolicyYAML))
	if err != nil {
		t.Fatalf("CompileSnapshot(1, schema fixture) returned error %v, want a compiled snapshot", err)
	}
	return snap
}

func newSchemaReloader(t *testing.T) *auth.Reloader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "security.yaml")
	if err := os.WriteFile(path, []byte(schemaPolicyYAML), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	reloader := auth.InitAuthReloader()
	if err := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-schema-contract-test")); err != nil {
		t.Fatalf("ConfigAuthReloader(%s) = %v, want it to accept the schema policy", path, err)
	}
	return reloader
}

// schemaActors resolves the five fixture principals against snap, keyed by the name each is
// referred to by in the expectation tables below.
func schemaActors(t *testing.T, snap auth.Snapshot) map[string]auth.Principal {
	t.Helper()
	return map[string]auth.Principal{
		"admin":        actor(t, snap, "sch-admin", "admin-secret"),
		"writer-alpha": actor(t, snap, "sch-writer-alpha", "writer-alpha-secret"),
		"reader-alpha": actor(t, snap, "sch-reader-alpha", "reader-alpha-secret"),
		"reader-all":   actor(t, snap, "sch-reader-all", "reader-all-secret"),
		"unbound":      actor(t, snap, "sch-unbound", "unbound-secret"),
	}
}

// measureIn is the fixture measure body. The same name is used in both groups so a scope
// leak shows up as the wrong group answering rather than as a different resource.
func measureIn(group string) *databasev1.Measure {
	return &databasev1.Measure{Metadata: &commonv1.Metadata{Group: group, Name: fixtureMeasure}}
}

// The three request factories below cover all seven registry families for the three
// structural request shapes the issue's R2 agrees on. Each returns the generated request type
// protoc produced for that service, so the oracle is a literal list of shapes rather than a
// structural assertion that would pass for a type the liaison never serves.
func registryListRequest(service, group string) any {
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceListRequest{Group: group}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceListRequest{Group: group}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceListRequest{Group: group}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceListRequest{Group: group}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: group}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceListRequest{Group: group}
	default:
		return &databasev1.PropertyRegistryServiceListRequest{Group: group}
	}
}

func registryGetRequest(service, group string) any {
	resourceMeta := &commonv1.Metadata{Group: group, Name: "fixture"}
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceGetRequest{Metadata: resourceMeta}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceGetRequest{Metadata: resourceMeta}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceGetRequest{Metadata: resourceMeta}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceGetRequest{Metadata: resourceMeta}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceGetRequest{Metadata: resourceMeta}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceGetRequest{Metadata: resourceMeta}
	default:
		return &databasev1.PropertyRegistryServiceGetRequest{Metadata: resourceMeta}
	}
}

// registryExistRequest and registryDeleteRequest build the other two members of
// the Metadata.Group family, and registryUpdateRequest the second member of the
// resource-body family. R2 names three structural families, and the design's
// § 06 asks the coverage test to prove "the method and extractor remain
// paired" — driving one shape per family satisfies the first reading, every
// method the second. These make both true at once, which is cheaper than
// deciding which R2 meant.
func registryExistRequest(service, group string) any {
	resourceMeta := &commonv1.Metadata{Group: group, Name: "fixture"}
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceExistRequest{Metadata: resourceMeta}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceExistRequest{Metadata: resourceMeta}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceExistRequest{Metadata: resourceMeta}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceExistRequest{Metadata: resourceMeta}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceExistRequest{Metadata: resourceMeta}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceExistRequest{Metadata: resourceMeta}
	default:
		return &databasev1.PropertyRegistryServiceExistRequest{Metadata: resourceMeta}
	}
}

func registryDeleteRequest(service, group string) any {
	resourceMeta := &commonv1.Metadata{Group: group, Name: "fixture"}
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceDeleteRequest{Metadata: resourceMeta}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceDeleteRequest{Metadata: resourceMeta}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceDeleteRequest{Metadata: resourceMeta}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceDeleteRequest{Metadata: resourceMeta}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceDeleteRequest{Metadata: resourceMeta}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceDeleteRequest{Metadata: resourceMeta}
	default:
		return &databasev1.PropertyRegistryServiceDeleteRequest{Metadata: resourceMeta}
	}
}

func registryUpdateRequest(service, group string) any {
	resourceMeta := &commonv1.Metadata{Group: group, Name: "fixture"}
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceUpdateRequest{Stream: &databasev1.Stream{Metadata: resourceMeta}}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceUpdateRequest{Measure: &databasev1.Measure{Metadata: resourceMeta}}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceUpdateRequest{Trace: &databasev1.Trace{Metadata: resourceMeta}}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceUpdateRequest{IndexRule: &databasev1.IndexRule{Metadata: resourceMeta}}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
			IndexRuleBinding: &databasev1.IndexRuleBinding{Metadata: resourceMeta},
		}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceUpdateRequest{
			TopNAggregation: &databasev1.TopNAggregation{Metadata: resourceMeta},
		}
	default:
		return &databasev1.PropertyRegistryServiceUpdateRequest{Property: &databasev1.Property{Metadata: resourceMeta}}
	}
}

func registryCreateRequest(service, group string) any {
	resourceMeta := &commonv1.Metadata{Group: group, Name: "fixture"}
	switch service {
	case streamRegistryService:
		return &databasev1.StreamRegistryServiceCreateRequest{Stream: &databasev1.Stream{Metadata: resourceMeta}}
	case measureRegistryService:
		return &databasev1.MeasureRegistryServiceCreateRequest{Measure: &databasev1.Measure{Metadata: resourceMeta}}
	case traceRegistryService:
		return &databasev1.TraceRegistryServiceCreateRequest{Trace: &databasev1.Trace{Metadata: resourceMeta}}
	case indexRuleRegistryService:
		return &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: &databasev1.IndexRule{Metadata: resourceMeta}}
	case indexRuleBindingRegistryService:
		return &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
			IndexRuleBinding: &databasev1.IndexRuleBinding{Metadata: resourceMeta},
		}
	case topNAggregationRegistryService:
		return &databasev1.TopNAggregationRegistryServiceCreateRequest{
			TopNAggregation: &databasev1.TopNAggregation{Metadata: resourceMeta},
		}
	default:
		return &databasev1.PropertyRegistryServiceCreateRequest{Property: &databasev1.Property{Metadata: resourceMeta}}
	}
}

// TestSchemaR1_ExactAndWildcardScopeDecisions proves R1: an exact grant admits its own group
// and nothing else, a wildcard grant admits every group including one created after the
// policy was loaded, a wrong scope is denied, and an unauthorized existence check is denied
// rather than answered. Every cell below is read off issue #14015's R1 and the built-in role
// definitions of the #13994 design; none is recomputed the way the decision will compute it.
func TestSchemaR1_ExactAndWildcardScopeDecisions(t *testing.T) {
	snap := schemaSnapshot(t)
	table := policyTable(t)
	actors := schemaActors(t, snap)

	const (
		allow   = liaisongrpc.DecisionAllow
		deny    = liaisongrpc.DecisionDeny
		invalid = liaisongrpc.DecisionInvalidRequest
	)
	for _, tc := range []struct {
		request                                             any
		method                                              string
		note                                                string
		admin, writerAlpha, readerAlpha, readerAll, unbound liaisongrpc.Decision
	}{
		// Group point reads take their scope from the request's own group field (B1).
		{
			method: mGroupGet, note: "alpha", request: &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha},
			admin: allow, writerAlpha: allow, readerAlpha: allow, readerAll: allow, unbound: deny,
		},
		{
			method: mGroupGet, note: "beta", request: &databasev1.GroupRegistryServiceGetRequest{Group: groupBeta},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		{
			method: mGroupExist, note: "beta is denied, never answered false",
			request: &databasev1.GroupRegistryServiceExistRequest{Group: groupBeta},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		// A wildcard grant covers a group nobody named when the policy was compiled.
		{
			method: mGroupGet, note: "gamma, created after policy load",
			request: &databasev1.GroupRegistryServiceGetRequest{Group: groupGamma},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		// Exact match only: no prefix, substring or case-folded match may admit a caller.
		{
			method: mGroupGet, note: "case-folded alpha",
			request: &databasev1.GroupRegistryServiceGetRequest{Group: "RBAC-ALPHA"},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		{
			method: mGroupGet, note: "alpha with a suffix",
			request: &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha + "-extra"},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		// Group upserts take their scope from Group.Metadata.Name, never Metadata.Group (B2).
		{
			method: mGroupCreate, note: "alpha", request: &databasev1.GroupRegistryServiceCreateRequest{
				Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupAlpha}},
			},
			admin: allow, writerAlpha: allow, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		{
			method: mGroupUpdate, note: "beta", request: &databasev1.GroupRegistryServiceUpdateRequest{
				Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupBeta}},
			},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		// A Group body whose Metadata.Group names an allowed group must not smuggle a write
		// into a group the caller has no grant on: only Metadata.Name is the resource.
		{
			method: mGroupCreate, note: "beta named, alpha in Metadata.Group",
			request: &databasev1.GroupRegistryServiceCreateRequest{
				Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupBeta, Group: groupAlpha}},
			},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		// Group deletion is decided before any dry-run, force or deletion-task logic (B3).
		{
			method: mGroupDelete, note: "alpha, force",
			request: &databasev1.GroupRegistryServiceDeleteRequest{Group: groupAlpha, Force: true},
			admin:   allow, writerAlpha: allow, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		{
			method: mGroupDelete, note: "beta, dry run",
			request: &databasev1.GroupRegistryServiceDeleteRequest{Group: groupBeta, DryRun: true},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		// Registry List takes its scope from the request's own group field (B4).
		{
			method: mMeasureSchemaList, note: "alpha", request: &databasev1.MeasureRegistryServiceListRequest{Group: groupAlpha},
			admin: allow, writerAlpha: allow, readerAlpha: allow, readerAll: allow, unbound: deny,
		},
		{
			method: mMeasureSchemaList, note: "beta", request: &databasev1.MeasureRegistryServiceListRequest{Group: groupBeta},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		// Registry point reads take their scope from Metadata.Group (B5).
		{
			method: mMeasureSchemaGet, note: "alpha", request: &databasev1.MeasureRegistryServiceGetRequest{
				Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure},
			},
			admin: allow, writerAlpha: allow, readerAlpha: allow, readerAll: allow, unbound: deny,
		},
		{
			method: mMeasureSchemaExist, note: "beta is denied, never answered false",
			request: &databasev1.MeasureRegistryServiceExistRequest{
				Metadata: &commonv1.Metadata{Group: groupBeta, Name: fixtureMeasure},
			},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		// Registry upserts take their scope from the resource body's Metadata.Group (B6).
		{
			method: mMeasureSchemaNew, note: "alpha", request: &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: measureIn(groupAlpha),
			},
			admin: allow, writerAlpha: allow, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		{
			method: mMeasureSchemaSet, note: "beta", request: &databasev1.MeasureRegistryServiceUpdateRequest{
				Measure: measureIn(groupBeta),
			},
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		// Registry deletion takes its scope from Metadata.Group (B7).
		{
			method: mMeasureSchemaDrop, note: "alpha", request: &databasev1.MeasureRegistryServiceDeleteRequest{
				Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure},
			},
			admin: allow, writerAlpha: allow, readerAlpha: deny, readerAll: deny, unbound: deny,
		},
		// A malformed request from an authenticated caller is malformed, not unauthorized,
		// and that answer does not depend on what the caller would have been allowed.
		{
			method: mGroupGet, note: "empty group", request: &databasev1.GroupRegistryServiceGetRequest{},
			admin: invalid, writerAlpha: invalid, readerAlpha: invalid, readerAll: invalid, unbound: invalid,
		},
		{
			method: mMeasureSchemaNew, note: "nil resource body", request: &databasev1.MeasureRegistryServiceCreateRequest{},
			admin: invalid, writerAlpha: invalid, readerAlpha: invalid, readerAll: invalid, unbound: invalid,
		},
	} {
		for _, who := range []struct {
			name string
			want liaisongrpc.Decision
		}{
			{"admin", tc.admin},
			{"writer-alpha", tc.writerAlpha},
			{"reader-alpha", tc.readerAlpha},
			{"reader-all", tc.readerAll},
			{"unbound", tc.unbound},
		} {
			got, reason := table.Authorize(snap, actors[who.name], tc.method, tc.request)
			if got != who.want {
				t.Errorf("Authorize(%s, %s, %s) = %v (%s), want %v", who.name, tc.method, tc.note, got, reason, who.want)
			}
		}
	}
}

// TestSchemaR2_ScopeFamilyClassification proves R2's classification half: every method the
// liaison serves is paired with exactly one scope family, and the pairing is the one the
// #13994 API policy map and the B1-B10 round catalog specify. The per-family counts below are
// hand-counted from that map — 7 registry families times the methods each round names — so a
// method that silently changes family, or a new RPC that inherits one, fails here.
func TestSchemaR2_ScopeFamilyClassification(t *testing.T) {
	table := policyTable(t)

	wantFamily := map[string]liaisongrpc.ScopeFamily{}
	for _, method := range []string{mGroupGet, mGroupExist, mGroupDelete} {
		wantFamily[method] = liaisongrpc.ScopeDirectGroup
	}
	for _, method := range []string{mGroupCreate, mGroupUpdate} {
		wantFamily[method] = liaisongrpc.ScopeGroupBodyName
	}
	wantFamily[mGroupList] = liaisongrpc.ScopeVisibleGroups
	for _, service := range registryServices {
		wantFamily[registryMethod(service, "List")] = liaisongrpc.ScopeDirectGroup
		for _, method := range []string{"Get", "Exist", "Delete"} {
			wantFamily[registryMethod(service, method)] = liaisongrpc.ScopeMetadataGroup
		}
		for _, method := range []string{"Create", "Update"} {
			wantFamily[registryMethod(service, method)] = liaisongrpc.ScopeResourceMetadataGroup
		}
	}
	wantFamily[mAwaitSchemaApplied] = liaisongrpc.ScopeSchemaKeys
	wantFamily[mAwaitSchemaDeleted] = liaisongrpc.ScopeSchemaKeys
	wantFamily[mAwaitRevision] = liaisongrpc.ScopeGlobal

	familyCounts := make(map[liaisongrpc.ScopeFamily]int)
	for _, policy := range table {
		familyCounts[policy.Scope]++
		want, classified := wantFamily[policy.FullMethod]
		if !classified {
			continue
		}
		if policy.Scope != want {
			t.Errorf("policy for %s has scope family %d, want %d", policy.FullMethod, policy.Scope, want)
		}
	}
	// The four data families and the eleventh direct-group method arrived with issue #14016,
	// and are hand-counted from the same API policy map: five repeated-group native reads
	// (Stream, Measure and Trace Query, Measure TopN, Property Query); Property Apply, whose
	// extractor reads the group off the property body; the three streaming writes, whose
	// groups arrive frame by frame; ByDBQL, decided after its transformation; and Property
	// Delete, which the map's directGroup row names alongside Group Get and registry List.
	// Those eleven leave only the four methods the map classifies as authenticated or health
	// with no scope family at all, so a permission-bearing method can no longer be
	// unclassified and the loop below has nothing left to forgive.
	for family, want := range map[liaisongrpc.ScopeFamily]int{
		liaisongrpc.ScopeGlobal:                13,
		liaisongrpc.ScopeDirectGroup:           11,
		liaisongrpc.ScopeGroupBodyName:         2,
		liaisongrpc.ScopeMetadataGroup:         21,
		liaisongrpc.ScopeResourceMetadataGroup: 14,
		liaisongrpc.ScopeSchemaKeys:            2,
		liaisongrpc.ScopeVisibleGroups:         1,
		liaisongrpc.ScopeRepeatedGroups:        5,
		liaisongrpc.ScopePropertyGroup:         1,
		liaisongrpc.ScopeFrameGroups:           3,
		liaisongrpc.ScopePostTransform:         1,
		liaisongrpc.ScopeUnspecified:           4,
	} {
		if familyCounts[family] != want {
			t.Errorf("scope family %d classifies %d methods, want %d", family, familyCounts[family], want)
		}
	}
	for _, policy := range table {
		if policy.Access != liaisongrpc.MethodAccessPermission || !policy.Activated {
			continue
		}
		if policy.Scope == liaisongrpc.ScopeUnspecified {
			t.Errorf("policy for %s is activated with no scope family, which would decide it globally", policy.FullMethod)
		}
	}
}

// TestSchemaR2_RequestScopesReadsTheAgreedRequestShapes proves R2's extraction half: each
// family reads its groups out of the request shape the round catalog names for it and out of
// no other, and a request it cannot be read from is reported as unresolvable rather than as
// an empty scope set that would collapse to a global decision. Every want below is written
// out by hand from the request literal beside it.
func TestSchemaR2_RequestScopesReadsTheAgreedRequestShapes(t *testing.T) {
	for _, tc := range []struct {
		request any
		name    string
		want    []string
		family  liaisongrpc.ScopeFamily
	}{
		{
			name: "direct group on a Group point read", family: liaisongrpc.ScopeDirectGroup,
			request: &databasev1.GroupRegistryServiceGetRequest{Group: groupBeta}, want: []string{groupBeta},
		},
		{
			name: "direct group on a Group delete", family: liaisongrpc.ScopeDirectGroup,
			request: &databasev1.GroupRegistryServiceDeleteRequest{Group: groupAlpha, Force: true}, want: []string{groupAlpha},
		},
		{
			name: "group body name, not the body's Metadata.Group", family: liaisongrpc.ScopeGroupBodyName,
			request: &databasev1.GroupRegistryServiceCreateRequest{
				Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupAlpha, Group: groupBeta}},
			},
			want: []string{groupAlpha},
		},
		{
			name: "metadata group", family: liaisongrpc.ScopeMetadataGroup,
			request: &databasev1.MeasureRegistryServiceGetRequest{
				Metadata: &commonv1.Metadata{Group: groupBeta, Name: fixtureMeasure},
			},
			want: []string{groupBeta},
		},
		{
			name: "resource metadata group", family: liaisongrpc.ScopeResourceMetadataGroup,
			request: &databasev1.MeasureRegistryServiceCreateRequest{Measure: measureIn(groupAlpha)},
			want:    []string{groupAlpha},
		},
		{
			name: "a global method addresses no group", family: liaisongrpc.ScopeGlobal,
			request: &schemav1.AwaitRevisionAppliedRequest{MinRevision: 7}, want: nil,
		},
		{
			name: "Group.List addresses no group and is filtered afterwards", family: liaisongrpc.ScopeVisibleGroups,
			request: &databasev1.GroupRegistryServiceListRequest{}, want: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := liaisongrpc.RequestScopes(tc.family, tc.request)
			if err != nil {
				t.Fatalf("RequestScopes(%d, %T) returned %v, want %v", tc.family, tc.request, err, tc.want)
			}
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("RequestScopes(%d, %T) = %v, want %v", tc.family, tc.request, got, tc.want)
			}
		})
	}

	t.Run("every registry family covers all seven services", func(t *testing.T) {
		for _, service := range registryServices {
			for _, shape := range []struct {
				request any
				family  liaisongrpc.ScopeFamily
			}{
				{family: liaisongrpc.ScopeDirectGroup, request: registryListRequest(service, groupAlpha)},
				{family: liaisongrpc.ScopeMetadataGroup, request: registryGetRequest(service, groupAlpha)},
				{family: liaisongrpc.ScopeMetadataGroup, request: registryExistRequest(service, groupAlpha)},
				{family: liaisongrpc.ScopeMetadataGroup, request: registryDeleteRequest(service, groupAlpha)},
				{family: liaisongrpc.ScopeResourceMetadataGroup, request: registryCreateRequest(service, groupAlpha)},
				{family: liaisongrpc.ScopeResourceMetadataGroup, request: registryUpdateRequest(service, groupAlpha)},
			} {
				got, err := liaisongrpc.RequestScopes(shape.family, shape.request)
				if err != nil {
					t.Errorf("RequestScopes(%d, %T) returned %v, want [%s]", shape.family, shape.request, err, groupAlpha)
					continue
				}
				if !reflect.DeepEqual(got, []string{groupAlpha}) {
					t.Errorf("RequestScopes(%d, %T) = %v, want [%s]", shape.family, shape.request, got, groupAlpha)
				}
			}
		}
	})

	t.Run("unresolvable requests", func(t *testing.T) {
		namedMetadata := &commonv1.Metadata{Name: fixtureMeasure}
		for _, tc := range []struct {
			request any
			name    string
			family  liaisongrpc.ScopeFamily
		}{
			{name: "nil request", family: liaisongrpc.ScopeDirectGroup, request: nil},
			{
				name: "empty direct group", family: liaisongrpc.ScopeDirectGroup,
				request: &databasev1.GroupRegistryServiceGetRequest{},
			},
			{
				name: "whitespace direct group", family: liaisongrpc.ScopeDirectGroup,
				request: &databasev1.GroupRegistryServiceGetRequest{Group: "  "},
			},
			{
				name: "nil group body", family: liaisongrpc.ScopeGroupBodyName,
				request: &databasev1.GroupRegistryServiceCreateRequest{},
			},
			{
				name: "group body with no name", family: liaisongrpc.ScopeGroupBodyName,
				request: &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{}},
			},
			{
				name: "nil metadata", family: liaisongrpc.ScopeMetadataGroup,
				request: &databasev1.MeasureRegistryServiceGetRequest{},
			},
			{
				name: "metadata with no group", family: liaisongrpc.ScopeMetadataGroup,
				request: &databasev1.MeasureRegistryServiceGetRequest{Metadata: namedMetadata},
			},
			{
				name: "nil resource body", family: liaisongrpc.ScopeResourceMetadataGroup,
				request: &databasev1.MeasureRegistryServiceCreateRequest{},
			},
			{
				name: "resource with nil metadata", family: liaisongrpc.ScopeResourceMetadataGroup,
				request: &databasev1.MeasureRegistryServiceCreateRequest{Measure: &databasev1.Measure{}},
			},
			{
				name: "request of the wrong family", family: liaisongrpc.ScopeMetadataGroup,
				request: &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha},
			},
			{
				name: "a family with no extractor", family: liaisongrpc.ScopeUnspecified,
				request: &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha},
			},
			{
				name: "schema key with no group", family: liaisongrpc.ScopeSchemaKeys,
				request: &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{{Kind: "measure", Name: "m"}}},
			},
			{
				name: "group-kind key with no name", family: liaisongrpc.ScopeSchemaKeys,
				request: &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{{Kind: "group"}}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := liaisongrpc.RequestScopes(tc.family, tc.request)
				if !errors.Is(err, liaisongrpc.ErrScopeUnresolvable) {
					t.Fatalf("RequestScopes(%d, %T) = %v, %v; want an error wrapping ErrScopeUnresolvable",
						tc.family, tc.request, got, err)
				}
				if len(got) != 0 {
					t.Errorf("RequestScopes(%d, %T) returned scopes %v alongside its error, want none", tc.family, tc.request, got)
				}
			})
		}
	})
}

// TestSchemaR3_DeniedMutationsNeverReachTheHandler proves R3's side-effect half at the
// interceptor seam: the decision precedes the handler, so a denied create, update or delete
// cannot have written metadata, opened a deletion task or left a tombstone — there was no
// handler invocation in which to do so. The status codes are what a client observes, so they
// are part of the contract: a wrong scope is PermissionDenied and a request carrying no
// resolvable group is InvalidArgument.
func TestSchemaR3_DeniedMutationsNeverReachTheHandler(t *testing.T) {
	interceptor := liaisongrpc.NewAuthorizationInterceptor(newSchemaReloader(t), policyTable(t), &recordingObserver{})

	for _, tc := range []struct {
		request  any
		method   string
		name     string
		user     string
		password string
		wantCode codes.Code
	}{
		{
			name: "reader cannot create a group", method: mGroupCreate,
			user: "sch-reader-alpha", password: "reader-alpha-secret",
			request: &databasev1.GroupRegistryServiceCreateRequest{
				Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupAlpha}},
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "alpha writer cannot delete the beta group", method: mGroupDelete,
			user: "sch-writer-alpha", password: "writer-alpha-secret",
			request:  &databasev1.GroupRegistryServiceDeleteRequest{Group: groupBeta, Force: true},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "alpha writer cannot create a beta measure", method: mMeasureSchemaNew,
			user: "sch-writer-alpha", password: "writer-alpha-secret",
			request:  &databasev1.MeasureRegistryServiceCreateRequest{Measure: measureIn(groupBeta)},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "reader cannot delete an alpha measure", method: mMeasureSchemaDrop,
			user: "sch-reader-alpha", password: "reader-alpha-secret",
			request: &databasev1.MeasureRegistryServiceDeleteRequest{
				Metadata: &commonv1.Metadata{Group: groupAlpha, Name: fixtureMeasure},
			},
			wantCode: codes.PermissionDenied,
		},
		{
			name: "an unresolvable group is malformed, not unauthorized", method: mMeasureSchemaNew,
			user: "sch-admin", password: "admin-secret",
			request:  &databasev1.MeasureRegistryServiceCreateRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "an empty group on a point read is malformed", method: mGroupGet,
			user: "sch-admin", password: "admin-secret",
			request:  &databasev1.GroupRegistryServiceGetRequest{},
			wantCode: codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(),
				metadata.Pairs("username", tc.user, "password", tc.password))
			handlerRan := false
			_, err := interceptor(ctx, tc.request, &grpclib.UnaryServerInfo{FullMethod: tc.method},
				func(context.Context, any) (any, error) {
					handlerRan = true
					return struct{}{}, nil
				})
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("%s on %s = %v, want %v", tc.user, tc.method, got, tc.wantCode)
			}
			if handlerRan {
				t.Errorf("the handler ran for a rejected %s call, want no side effect", tc.method)
			}
		})
	}

	t.Run("an allowed mutation reaches its handler", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("username", "sch-writer-alpha", "password", "writer-alpha-secret"))
		handlerRan := false
		_, err := interceptor(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: measureIn(groupAlpha)},
			&grpclib.UnaryServerInfo{FullMethod: mMeasureSchemaNew},
			func(context.Context, any) (any, error) {
				handlerRan = true
				return struct{}{}, nil
			})
		if err != nil {
			t.Fatalf("the alpha writer creating an alpha measure returned %v, want the handler's result", err)
		}
		if !handlerRan {
			t.Error("the handler did not run for an allowed create")
		}
	})
}

// TestSchemaR3_GroupListIsFilteredToVisibleScopes proves R3's visibility half: Group List
// admits any principal holding schema:read somewhere and rejects one holding it nowhere, and
// the response it returns names only the groups that principal's scopes cover. The expected
// lists are written out by hand from the fixture bindings, and a group created after the
// policy was compiled is included so wildcard visibility is not confused with a fixed list.
func TestSchemaR3_GroupListIsFilteredToVisibleScopes(t *testing.T) {
	snap := schemaSnapshot(t)
	table := policyTable(t)
	actors := schemaActors(t, snap)
	policy, exists := table.Policy(mGroupList)
	if !exists {
		t.Fatalf("the table does not classify %s", mGroupList)
	}

	unfiltered := &databasev1.GroupRegistryServiceListResponse{
		Group: []*commonv1.Group{
			{Metadata: &commonv1.Metadata{Name: groupAlpha}},
			{Metadata: &commonv1.Metadata{Name: groupBeta}},
			{Metadata: &commonv1.Metadata{Name: groupGamma}},
		},
	}
	for _, tc := range []struct {
		who         string
		wantVisible []string
		wantAdmit   liaisongrpc.Decision
	}{
		{who: "admin", wantAdmit: liaisongrpc.DecisionAllow, wantVisible: []string{groupAlpha, groupBeta, groupGamma}},
		{who: "reader-all", wantAdmit: liaisongrpc.DecisionAllow, wantVisible: []string{groupAlpha, groupBeta, groupGamma}},
		{who: "reader-alpha", wantAdmit: liaisongrpc.DecisionAllow, wantVisible: []string{groupAlpha}},
		{who: "writer-alpha", wantAdmit: liaisongrpc.DecisionAllow, wantVisible: []string{groupAlpha}},
		{who: "unbound", wantAdmit: liaisongrpc.DecisionDeny, wantVisible: nil},
	} {
		t.Run(tc.who, func(t *testing.T) {
			principal := actors[tc.who]
			admit, reason := table.Authorize(snap, principal, mGroupList, &databasev1.GroupRegistryServiceListRequest{})
			if admit != tc.wantAdmit {
				t.Fatalf("Authorize(%s, %s) = %v (%s), want %v", tc.who, mGroupList, admit, reason, tc.wantAdmit)
			}
			if tc.wantAdmit != liaisongrpc.DecisionAllow {
				return
			}
			filtered, ok := liaisongrpc.FilterResponse(snap, principal, policy, unfiltered).(*databasev1.GroupRegistryServiceListResponse)
			if !ok {
				t.Fatalf("FilterResponse(%s) did not return a Group List response", tc.who)
			}
			names := make([]string, 0, len(filtered.GetGroup()))
			for _, group := range filtered.GetGroup() {
				names = append(names, group.GetMetadata().GetName())
			}
			if !reflect.DeepEqual(names, tc.wantVisible) {
				t.Errorf("FilterResponse(%s) listed %v, want %v", tc.who, names, tc.wantVisible)
			}
		})
	}

	t.Run("the unfiltered response is not mutated", func(t *testing.T) {
		if len(unfiltered.GetGroup()) != 3 {
			t.Fatalf("filtering reduced the handler's own response to %d groups, want the original 3 untouched",
				len(unfiltered.GetGroup()))
		}
	})

	// A policy reload that lands while the handler is running must not widen or narrow what
	// this request sees: the filter answers from the snapshot the decision was taken against,
	// which is why it is passed one rather than reading the reloader itself.
	t.Run("filtering answers from the request's own snapshot", func(t *testing.T) {
		widened, compileErr := auth.CompileSnapshot(2, []byte(widenedSchemaPolicyYAML))
		if compileErr != nil {
			t.Fatalf("CompileSnapshot(2, widened fixture) returned %v, want a compiled snapshot", compileErr)
		}
		reader := actor(t, widened, "sch-reader-alpha", "reader-alpha-secret")
		newFiltered, ok := liaisongrpc.FilterResponse(widened, reader, policy, unfiltered).(*databasev1.GroupRegistryServiceListResponse)
		if !ok {
			t.Fatalf("FilterResponse against revision 2 did not return a Group List response")
		}
		if len(newFiltered.GetGroup()) != 3 {
			t.Fatalf("revision 2 widened the alpha reader to a wildcard, so it should list 3 groups, got %d",
				len(newFiltered.GetGroup()))
		}
		// The same principal filtered against revision 1 must still see only alpha.
		oldFiltered, ok := liaisongrpc.FilterResponse(snap, actors["reader-alpha"], policy, unfiltered).(*databasev1.GroupRegistryServiceListResponse)
		if !ok {
			t.Fatalf("FilterResponse against revision 1 did not return a Group List response")
		}
		if len(oldFiltered.GetGroup()) != 1 {
			t.Errorf("the revision-1 snapshot listed %d groups, want only %s; a filter must not read a newer revision",
				len(oldFiltered.GetGroup()), groupAlpha)
		}
	})

	t.Run("no other policy has its reply filtered", func(t *testing.T) {
		getPolicy, _ := table.Policy(mGroupGet)
		reply := &databasev1.GroupRegistryServiceGetResponse{Group: &commonv1.Group{Metadata: &commonv1.Metadata{Name: groupBeta}}}
		if got := liaisongrpc.FilterResponse(snap, actors["reader-alpha"], getPolicy, reply); got != any(reply) {
			t.Errorf("FilterResponse on a %s reply returned %v, want the reply unchanged", mGroupGet, got)
		}
	})
}

// TestSchemaR4_SchemaBarrierScopes proves R4: a key wait resolves one scope per key, taking a
// group-kind key's scope from SchemaKey.Name and every other kind's from SchemaKey.Group,
// deduplicates them, and requires the permission for all of them, while a revision wait names
// no group and is satisfied only by a wildcard schema read. A key list that names no group at
// all resolves to no scope, which makes it a wildcard question rather than a free pass.
func TestSchemaR4_SchemaBarrierScopes(t *testing.T) {
	t.Run("keys resolve to their groups", func(t *testing.T) {
		for _, tc := range []struct {
			request *schemav1.AwaitSchemaAppliedRequest
			name    string
			want    []string
		}{
			{
				name: "a group-kind key scopes to its name",
				request: &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{
					{Kind: "group", Name: groupAlpha},
				}},
				want: []string{groupAlpha},
			},
			{
				name: "every other kind scopes to its group",
				request: &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{
					{Kind: "measure", Group: groupAlpha, Name: fixtureMeasure},
				}},
				want: []string{groupAlpha},
			},
			{
				name: "duplicate and unordered keys collapse to one sorted set",
				request: &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{
					{Kind: "measure", Group: groupBeta, Name: fixtureMeasure},
					{Kind: "stream", Group: groupAlpha, Name: "segment"},
					{Kind: "measure", Group: groupBeta, Name: "service_resp_time"},
					{Kind: "group", Name: groupAlpha},
				}},
				want: []string{groupAlpha, groupBeta},
			},
			{
				name:    "no keys name no group",
				request: &schemav1.AwaitSchemaAppliedRequest{},
				want:    nil,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := liaisongrpc.RequestScopes(liaisongrpc.ScopeSchemaKeys, tc.request)
				if err != nil {
					t.Fatalf("RequestScopes(ScopeSchemaKeys, %v) returned %v, want %v", tc.request.GetKeys(), err, tc.want)
				}
				if len(got) == 0 && len(tc.want) == 0 {
					return
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("RequestScopes(ScopeSchemaKeys, %v) = %v, want %v", tc.request.GetKeys(), got, tc.want)
				}
			})
		}
	})

	snap := schemaSnapshot(t)
	table := policyTable(t)
	actors := schemaActors(t, snap)
	const (
		allow = liaisongrpc.DecisionAllow
		deny  = liaisongrpc.DecisionDeny
	)
	alphaKeys := &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{{Kind: "group", Name: groupAlpha}}}
	bothKeys := &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{
		{Kind: "group", Name: groupAlpha},
		{Kind: "group", Name: groupBeta},
	}}
	deleteBeta := &schemav1.AwaitSchemaDeletedRequest{Keys: []*schemav1.SchemaKey{
		{Kind: "measure", Group: groupBeta, Name: fixtureMeasure},
	}}
	for _, tc := range []struct {
		request                                             any
		method                                              string
		note                                                string
		admin, writerAlpha, readerAlpha, readerAll, unbound liaisongrpc.Decision
	}{
		{
			method: mAwaitSchemaApplied, note: "alpha only", request: alphaKeys,
			admin: allow, writerAlpha: allow, readerAlpha: allow, readerAll: allow, unbound: deny,
		},
		{
			method: mAwaitSchemaApplied, note: "alpha and beta is all-or-nothing", request: bothKeys,
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		{
			method: mAwaitSchemaDeleted, note: "beta", request: deleteBeta,
			admin: allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
		{
			method: mAwaitRevision, note: "a cluster-wide wait needs a wildcard read",
			request: &schemav1.AwaitRevisionAppliedRequest{MinRevision: 0},
			admin:   allow, writerAlpha: deny, readerAlpha: deny, readerAll: allow, unbound: deny,
		},
	} {
		for _, who := range []struct {
			name string
			want liaisongrpc.Decision
		}{
			{"admin", tc.admin},
			{"writer-alpha", tc.writerAlpha},
			{"reader-alpha", tc.readerAlpha},
			{"reader-all", tc.readerAll},
			{"unbound", tc.unbound},
		} {
			got, reason := table.Authorize(snap, actors[who.name], tc.method, tc.request)
			if got != who.want {
				t.Errorf("Authorize(%s, %s, %s) = %v (%s), want %v", who.name, tc.method, tc.note, got, reason, who.want)
			}
		}
	}
}

// The data half of R6 — "every method carrying a data permission is still fail-closed" — was
// true only while W-PR2 was the head of this series. Issue #14016 activates those eleven
// methods, so the invariant now lives in TestDataR6_EveryLiaisonMethodIsActivatedAndBounded,
// which asserts the same eleven-method oracle with the opposite expectation.

// TestSchemaR6_DecisionReasonsStayBounded proves that the reason this milestone adds joins a
// closed set rather than opening one: every reason the decision function can return is listed
// by DecisionReasons(), and the outcome label of the new invalid-request decision collapses
// into the same two-valued decision label the metric already uses.
func TestSchemaR6_DecisionReasonsStayBounded(t *testing.T) {
	bounded := make(map[liaisongrpc.DecisionReason]bool)
	for _, reason := range liaisongrpc.DecisionReasons() {
		if bounded[reason] {
			t.Errorf("DecisionReasons() lists %q twice", reason)
		}
		bounded[reason] = true
	}
	if !bounded[liaisongrpc.DecisionReasonInvalidRequest] {
		t.Errorf("DecisionReasons() omits %q", liaisongrpc.DecisionReasonInvalidRequest)
	}

	labels := make(map[string]bool)
	for _, label := range liaisongrpc.DecisionLabels() {
		labels[label] = true
	}
	if got := liaisongrpc.DecisionLabel(liaisongrpc.DecisionInvalidRequest); !labels[got] {
		t.Errorf("DecisionLabel(DecisionInvalidRequest) = %q, which is outside DecisionLabels() %v", got, liaisongrpc.DecisionLabels())
	}

	snap := schemaSnapshot(t)
	table := policyTable(t)
	actors := schemaActors(t, snap)
	for _, tc := range []struct {
		request any
		method  string
	}{
		{method: mGroupGet, request: &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha}},
		{method: mGroupGet, request: &databasev1.GroupRegistryServiceGetRequest{Group: groupBeta}},
		{method: mGroupGet, request: &databasev1.GroupRegistryServiceGetRequest{}},
		{method: mGroupList, request: &databasev1.GroupRegistryServiceListRequest{}},
		{method: mMeasureQuery, request: nil},
		{method: mGetClusterState, request: nil},
	} {
		for _, who := range []string{"admin", "reader-alpha", "unbound"} {
			if _, reason := table.Authorize(snap, actors[who], tc.method, tc.request); !bounded[reason] {
				t.Errorf("Authorize(%s, %s) reported %q, which is outside DecisionReasons()", who, tc.method, reason)
			}
		}
	}
}
