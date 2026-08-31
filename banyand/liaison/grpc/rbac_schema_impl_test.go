// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package grpc

import (
	"reflect"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// TestSchemaB1GroupPointReadPolicy activates Group point reads with the direct-group family.
func TestSchemaB1GroupPointReadPolicy(t *testing.T) {
	table := GlobalMethodPolicies()
	for _, method := range []string{
		"/banyandb.database.v1.GroupRegistryService/Get",
		"/banyandb.database.v1.GroupRegistryService/Exist",
	} {
		policy, exists := table.Policy(method)
		if !exists {
			t.Fatalf("policy for %s is missing", method)
		}
		if !policy.Activated {
			t.Errorf("policy for %s is not activated", method)
		}
		if policy.Scope != ScopeDirectGroup {
			t.Errorf("policy for %s has scope %d, want %d", method, policy.Scope, ScopeDirectGroup)
		}
	}
}

// TestSchemaB2GroupWritesUseBodyName keeps Group upserts bound to Metadata.Name.
func TestSchemaB2GroupWritesUseBodyName(t *testing.T) {
	request := &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{Metadata: &commonv1.Metadata{Group: "beta", Name: "alpha"}},
	}
	scopes, scopeErr := RequestScopes(ScopeGroupBodyName, request)
	if scopeErr != nil {
		t.Fatalf("RequestScopes(ScopeGroupBodyName, Group Create) = %v", scopeErr)
	}
	if !reflect.DeepEqual(scopes, []string{"alpha"}) {
		t.Errorf("Group Create scopes = %v, want [alpha]", scopes)
	}
	for _, method := range []string{
		"/banyandb.database.v1.GroupRegistryService/Create",
		"/banyandb.database.v1.GroupRegistryService/Update",
	} {
		assertSchemaPolicy(t, method, ScopeGroupBodyName)
	}
}

// TestSchemaB3GroupDeleteUsesDirectGroup keeps deletion independent of its operational flags.
func TestSchemaB3GroupDeleteUsesDirectGroup(t *testing.T) {
	request := &databasev1.GroupRegistryServiceDeleteRequest{Group: "alpha", DryRun: true, Force: true}
	scopes, scopeErr := RequestScopes(ScopeDirectGroup, request)
	if scopeErr != nil {
		t.Fatalf("RequestScopes(ScopeDirectGroup, Group Delete) = %v", scopeErr)
	}
	if !reflect.DeepEqual(scopes, []string{"alpha"}) {
		t.Errorf("Group Delete scopes = %v, want [alpha]", scopes)
	}
	assertSchemaPolicy(t, "/banyandb.database.v1.GroupRegistryService/Delete", ScopeDirectGroup)
}

// TestSchemaB4RegistryListsUseDirectGroup activates all seven registry List methods.
func TestSchemaB4RegistryListsUseDirectGroup(t *testing.T) {
	for _, service := range schemaRegistryServices {
		assertSchemaPolicy(t, registryPolicyMethod(service, "List"), ScopeDirectGroup)
	}
}

// TestSchemaB5RegistryPointReadsUseMetadataGroup activates all seven Get and Exist methods.
func TestSchemaB5RegistryPointReadsUseMetadataGroup(t *testing.T) {
	request := &databasev1.MeasureRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Group: "alpha", Name: "service"}}
	scopes, scopeErr := RequestScopes(ScopeMetadataGroup, request)
	if scopeErr != nil {
		t.Fatalf("RequestScopes(ScopeMetadataGroup, Measure Get) = %v", scopeErr)
	}
	if !reflect.DeepEqual(scopes, []string{"alpha"}) {
		t.Errorf("Measure Get scopes = %v, want [alpha]", scopes)
	}
	for _, service := range schemaRegistryServices {
		for _, method := range []string{"Get", "Exist"} {
			assertSchemaPolicy(t, registryPolicyMethod(service, method), ScopeMetadataGroup)
		}
	}
}

// TestSchemaB6RegistryWritesUseResourceMetadataGroup activates all seven Create and Update methods.
func TestSchemaB6RegistryWritesUseResourceMetadataGroup(t *testing.T) {
	request := &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{Metadata: &commonv1.Metadata{Group: "alpha", Name: "service"}},
	}
	scopes, scopeErr := RequestScopes(ScopeResourceMetadataGroup, request)
	if scopeErr != nil {
		t.Fatalf("RequestScopes(ScopeResourceMetadataGroup, Measure Create) = %v", scopeErr)
	}
	if !reflect.DeepEqual(scopes, []string{"alpha"}) {
		t.Errorf("Measure Create scopes = %v, want [alpha]", scopes)
	}
	for _, service := range schemaRegistryServices {
		for _, method := range []string{"Create", "Update"} {
			assertSchemaPolicy(t, registryPolicyMethod(service, method), ScopeResourceMetadataGroup)
		}
	}
}

// TestSchemaB7RegistryDeletesUseMetadataGroup activates all seven Delete methods.
func TestSchemaB7RegistryDeletesUseMetadataGroup(t *testing.T) {
	for _, service := range schemaRegistryServices {
		assertSchemaPolicy(t, registryPolicyMethod(service, "Delete"), ScopeMetadataGroup)
	}
}

// TestSchemaB8GroupListFiltersWithItsDecisionSnapshot preserves the handler's reply.
func TestSchemaB8GroupListFiltersWithItsDecisionSnapshot(t *testing.T) {
	snapshot := implementationSnapshot(t)
	principal, authenticated := snapshot.Authenticate("reader-alpha", "reader-secret")
	if !authenticated {
		t.Fatal("reader-alpha fixture did not authenticate")
	}
	policy, exists := GlobalMethodPolicies().Policy("/banyandb.database.v1.GroupRegistryService/List")
	if !exists {
		t.Fatal("Group List policy is missing")
	}
	reply := &databasev1.GroupRegistryServiceListResponse{Group: []*commonv1.Group{
		{Metadata: &commonv1.Metadata{Name: "alpha"}},
		{Metadata: &commonv1.Metadata{Name: "beta"}},
	}}
	filtered, ok := FilterResponse(snapshot, principal, policy, reply).(*databasev1.GroupRegistryServiceListResponse)
	if !ok {
		t.Fatal("FilterResponse returned a non-Group List reply")
	}
	if len(filtered.GetGroup()) != 1 || filtered.GetGroup()[0].GetMetadata().GetName() != "alpha" {
		t.Errorf("filtered Group List = %v, want only alpha", filtered.GetGroup())
	}
	if len(reply.GetGroup()) != 2 {
		t.Errorf("FilterResponse mutated the handler reply to %d groups, want 2", len(reply.GetGroup()))
	}
}

// TestSchemaB9SchemaBarrierKeysUseEveryResolvedGroup uses the group key name and canonicalizes scopes.
func TestSchemaB9SchemaBarrierKeysUseEveryResolvedGroup(t *testing.T) {
	request := &schemav1.AwaitSchemaAppliedRequest{Keys: []*schemav1.SchemaKey{
		{Kind: "measure", Group: "beta", Name: "service"},
		{Kind: "group", Name: "alpha"},
		{Kind: "measure", Group: "beta", Name: "latency"},
	}}
	scopes, scopeErr := RequestScopes(ScopeSchemaKeys, request)
	if scopeErr != nil {
		t.Fatalf("RequestScopes(ScopeSchemaKeys, AwaitSchemaApplied) = %v", scopeErr)
	}
	if !reflect.DeepEqual(scopes, []string{"alpha", "beta"}) {
		t.Errorf("SchemaBarrier scopes = %v, want [alpha beta]", scopes)
	}
	assertSchemaPolicy(t, "/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaApplied", ScopeSchemaKeys)
	assertSchemaPolicy(t, "/banyandb.schema.v1.SchemaBarrierService/AwaitSchemaDeleted", ScopeSchemaKeys)
}

// TestSchemaB10RevisionWaitUsesGlobalScope requires the wildcard/global form of schema read.
func TestSchemaB10RevisionWaitUsesGlobalScope(t *testing.T) {
	assertSchemaPolicy(t, "/banyandb.schema.v1.SchemaBarrierService/AwaitRevisionApplied", ScopeGlobal)
}

var schemaRegistryServices = []string{
	"StreamRegistryService", "MeasureRegistryService", "TraceRegistryService", "IndexRuleRegistryService",
	"IndexRuleBindingRegistryService", "TopNAggregationRegistryService", "PropertyRegistryService",
}

func registryPolicyMethod(service, method string) string {
	return "/banyandb.database.v1." + service + "/" + method
}

func assertSchemaPolicy(t *testing.T, method string, scope ScopeFamily) {
	t.Helper()
	policy, exists := GlobalMethodPolicies().Policy(method)
	if !exists {
		t.Fatalf("policy for %s is missing", method)
	}
	if !policy.Activated {
		t.Errorf("policy for %s is not activated", method)
	}
	if policy.Scope != scope {
		t.Errorf("policy for %s has scope %d, want %d", method, policy.Scope, scope)
	}
}

func implementationSnapshot(t *testing.T) auth.Snapshot {
	t.Helper()
	const policy = `
users:
  - username: "reader-alpha"
    password: "reader-secret"
rbac:
  enabled: true
  bindings:
    - principal: "reader-alpha"
      role: "reader"
      groups: ["alpha"]
`
	snapshot, compileErr := auth.CompileSnapshot(1, []byte(policy))
	if compileErr != nil {
		t.Fatalf("CompileSnapshot() = %v", compileErr)
	}
	return snapshot
}
