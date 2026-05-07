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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
)

// fakeRevisionRepo implements registry.RevisionRepository with a controllable
// per-key map. Tests use it to simulate the eventCh-retry leak the gate must
// close: a state where the entityRepo locator already advanced to R but the
// schemaRepo (and therefore the registry) has not.
type fakeRevisionRepo struct {
	keys map[string]int64
	mu   sync.RWMutex
}

func newFakeRevisionRepo() *fakeRevisionRepo {
	return &fakeRevisionRepo{keys: make(map[string]int64)}
}

func (f *fakeRevisionRepo) keyOf(kind schema.Kind, group, name string) string {
	return kind.String() + "|" + group + "|" + name
}

func (f *fakeRevisionRepo) set(kind schema.Kind, group, name string, rev int64) {
	f.mu.Lock()
	f.keys[f.keyOf(kind, group, name)] = rev
	f.mu.Unlock()
}

// ResourceRevision implements registry.RevisionRepository.
func (f *fakeRevisionRepo) ResourceRevision(kind schema.Kind, group, name string) (int64, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	rev, ok := f.keys[f.keyOf(kind, group, name)]
	return rev, ok
}

// IsAbsent implements registry.RevisionRepository.
func (f *fakeRevisionRepo) IsAbsent(kind schema.Kind, group, name string) bool {
	_, ok := f.ResourceRevision(kind, group, name)
	return !ok
}

// fakeRepoWithRegistry satisfies metadata.Repo via an embedded nil interface
// and adds NodeRepoRegistry so the gate's schemaRevisionRegistry helper can
// pick up the per-node registry without forcing the test to stub every
// metadata.Service method. The embedded metadata.Repo is never invoked from
// the gate path under test.
type fakeRepoWithRegistry struct {
	metadata.Repo
	reg *registry.NodeRepoRegistry
}

func (f *fakeRepoWithRegistry) NodeRepoRegistry() *registry.NodeRepoRegistry { return f.reg }

// newRepoWithRegisteredFake builds a metadata.Repo whose NodeRepoRegistry has
// `repo` bound under `kinds`. Returns the wired metadata.Repo so it can be
// dropped into discoveryService.metadataRepo for gate tests.
func newRepoWithRegisteredFake(kinds schema.Kind, repo *fakeRevisionRepo) metadata.Repo {
	reg := registry.NewNodeRepoRegistry()
	reg.Register(kinds, repo)
	return &fakeRepoWithRegistry{reg: reg}
}

// withMetadataRepo wires svc.discoveryService.metadataRepo to repo. The
// helpers in measure_write_test.go / stream_write_test.go / trace_write_test.go
// leave the field nil; these tests overwrite it so the gate consults the
// registry instead of falling through to the locator.
func withMetadataRepoMeasure(svc *measureService, repo metadata.Repo) *measureService {
	svc.discoveryService.metadataRepo = repo
	return svc
}

func withMetadataRepoStream(svc *streamService, repo metadata.Repo) *streamService {
	svc.discoveryService.metadataRepo = repo
	return svc
}

func withMetadataRepoTrace(svc *traceService, repo metadata.Repo) *traceService {
	svc.discoveryService.metadataRepo = repo
	return svc
}

// --- Write gate ---------------------------------------------------------.

// TestWriteGate_Measure_RegistryLagsLocator_GateRunsBoundedAwait reproduces the
// eventCh-retry leak: the entityRepo locator advanced to R but the
// schemaRepo-backed registry did not, so the bare locator-only gate would
// falsely succeed at R. With the registry-aware gate the request is treated
// as ahead-of-cache; the bounded await fires; with the registry never
// advancing, the gate returns STATUS_SCHEMA_NOT_APPLIED.
func TestWriteGate_Measure_RegistryLagsLocator_GateRunsBoundedAwait(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id) // locator at seededLocatorRepoRev (=100)
	repo := newFakeRevisionRepo()
	repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev-1) // registry one rev behind

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	svc := withMetadataRepoMeasure(newTestMeasureService(er, 50*time.Millisecond), repoWithReg)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)
	require.Len(t, mock.replies, 1)
	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED.String(), mock.replies[0].Status)
}

// TestWriteGate_Measure_RegistryAdvancesDuringAwait_GateSucceeds asserts the
// registry-aware gate succeeds when the schemaRepo catches up inside the
// bounded await window — the "watch event finally landed before timeout"
// case the cluster barrier already handles for AwaitRevisionApplied.
func TestWriteGate_Measure_RegistryAdvancesDuringAwait_GateSucceeds(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id)
	repo := newFakeRevisionRepo()
	repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev-1)

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	svc := withMetadataRepoMeasure(newTestMeasureService(er, 500*time.Millisecond), repoWithReg)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	go func() {
		time.Sleep(20 * time.Millisecond)
		repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev)
	}()

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies, "no error reply should be sent when the registry catches up in time")
}

// TestWriteGate_Measure_RegistryEqualsClient_GateSucceeds_EvenWhenLocatorLags
// asserts that when the registry already holds the client's revision, the
// gate accepts the write even if the locator has not yet been refreshed —
// the inverse of the leak: if the schemaRepo says R, the executor will see R,
// regardless of whether the locator handler fired first.
func TestWriteGate_Measure_RegistryEqualsClient_GateSucceeds_EvenWhenLocatorLags(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id) // locator at 100
	repo := newFakeRevisionRepo()
	repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev) // registry at 100

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	svc := withMetadataRepoMeasure(newTestMeasureService(er, 50*time.Millisecond), repoWithReg)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}

// TestWriteGate_Measure_NoRegistry_FallsBackToLocator asserts the existing
// behavior is preserved on metadata.Repo values that do not expose
// NodeRepoRegistry — the locator's ModRevision is the comparison source and
// the prior test fixtures (see measure_write_test.go) still hold.
func TestWriteGate_Measure_NoRegistry_FallsBackToLocator(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id) // locator at 100
	svc := newTestMeasureService(er, 50*time.Millisecond)
	mock := &mockBidiServer[measurev1.WriteRequest, measurev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validMeasureWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}

func TestWriteGate_Stream_RegistryLagsLocator_GateRunsBoundedAwait(t *testing.T) {
	id := identity{group: "g", name: "s"}
	er := seededLocatorRepo(id)
	repo := newFakeRevisionRepo()
	repo.set(schema.KindStream, id.group, id.name, seededLocatorRepoRev-1)

	repoWithReg := newRepoWithRegisteredFake(schema.KindStream, repo)
	svc := withMetadataRepoStream(newTestStreamService(er, 50*time.Millisecond), repoWithReg)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)
}

func TestWriteGate_Trace_RegistryLagsTrace_GateRunsBoundedAwait(t *testing.T) {
	id := identity{group: "g", name: "tr"}
	er := seededTraceRepo(id, seededLocatorRepoRev)
	repo := newFakeRevisionRepo()
	repo.set(schema.KindTrace, id.group, id.name, seededLocatorRepoRev-1)

	repoWithReg := newRepoWithRegisteredFake(schema.KindTrace, repo)
	svc := withMetadataRepoTrace(newTestTraceService(er, 50*time.Millisecond), repoWithReg)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: seededLocatorRepoRev}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)
}

// --- Query gate ---------------------------------------------------------.

// TestQueryGate_Measure_RegistryLag_ReportsSchemaNotApplied: per-group gate
// receives a client revision that the locator says is satisfied but the
// registry says is one behind. The receiving liaison must report
// STATUS_SCHEMA_NOT_APPLIED so a follow-up routed through this liaison sees
// the same verdict the data-node executor would produce on a forwarded
// sub-query.
func TestQueryGate_Measure_RegistryLag_ReportsSchemaNotApplied(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := seededLocatorRepo(id) // locator at 100
	repo := newFakeRevisionRepo()
	repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev-1)

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	measureReg := schemaRevisionRegistry(repoWithReg)
	require.NotNil(t, measureReg)

	statuses, shortCircuit := checkQueryGate(
		[]string{id.group},
		id.name,
		map[string]int64{id.group: seededLocatorRepoRev},
		func(name, group string) (int64, bool) {
			loc, ok := er.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(measureReg, schema.KindMeasure, group, name, loc.ModRevision, ok)
		},
		50*time.Millisecond,
	)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, statuses[id.group])
	assert.True(t, shortCircuit)
}

// TestQueryGate_Measure_RegistryEqual_ReportsSucceed: registry holds the
// client's revision; gate succeeds without short-circuit even when the
// locator lags (inverse of the leak).
func TestQueryGate_Measure_RegistryEqual_ReportsSucceed(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := newEmptyEntityRepo() // locator missing
	repo := newFakeRevisionRepo()
	repo.set(schema.KindMeasure, id.group, id.name, seededLocatorRepoRev)

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	measureReg := schemaRevisionRegistry(repoWithReg)

	statuses, shortCircuit := checkQueryGate(
		[]string{id.group},
		id.name,
		map[string]int64{id.group: seededLocatorRepoRev},
		func(name, group string) (int64, bool) {
			loc, ok := er.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(measureReg, schema.KindMeasure, group, name, loc.ModRevision, ok)
		},
		50*time.Millisecond,
	)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, statuses[id.group])
	assert.False(t, shortCircuit)
}

// TestQueryGate_Measure_RegistryAuthoritative_BothMissing_ReportsNotFound:
// when the registry is authoritative for the kind, the locator is missing,
// and the registry has no entry, the gate returns STATUS_NOT_FOUND — not
// SCHEMA_NOT_APPLIED — so callers get the same shape they got pre-Option-C
// for an unknown resource.
func TestQueryGate_Measure_RegistryAuthoritative_BothMissing_ReportsNotFound(t *testing.T) {
	id := identity{group: "g", name: "m"}
	er := newEmptyEntityRepo()
	repo := newFakeRevisionRepo()
	// repo registered but holds no entry for (KindMeasure, g, m); locator
	// also empty. Registry is authoritative for KindMeasure and reports
	// absent → resolveQueryGateRevision should mirror locator existence.

	repoWithReg := newRepoWithRegisteredFake(schema.KindMeasure, repo)
	measureReg := schemaRevisionRegistry(repoWithReg)

	statuses, shortCircuit := checkQueryGate(
		[]string{id.group},
		id.name,
		map[string]int64{id.group: seededLocatorRepoRev},
		func(name, group string) (int64, bool) {
			loc, ok := er.getLocator(identity{name: name, group: group})
			return resolveQueryGateRevision(measureReg, schema.KindMeasure, group, name, loc.ModRevision, ok)
		},
		50*time.Millisecond,
	)

	assert.Equal(t, modelv1.Status_STATUS_NOT_FOUND, statuses[id.group])
	assert.True(t, shortCircuit)
}

// --- Helper-level tests --------------------------------------------------.

func TestResolveSchemaRevision_RegistryNotTrackingKind_ReturnsFallback(t *testing.T) {
	repo := newFakeRevisionRepo()
	// Register the fake against KindStream only; KindMeasure is un-tracked.
	reg := registry.NewNodeRepoRegistry()
	reg.Register(schema.KindStream, repo)

	got := resolveSchemaRevision(reg, schema.KindMeasure, "g", "m", seededLocatorRepoRev)
	assert.Equal(t, seededLocatorRepoRev, got, "un-tracked kinds should return the caller-provided fallback")
}

func TestResolveSchemaRevision_RegistryAuthoritative_KeyAbsent_ReturnsZero(t *testing.T) {
	repo := newFakeRevisionRepo()
	reg := registry.NewNodeRepoRegistry()
	reg.Register(schema.KindMeasure, repo)
	// No key set on repo.

	got := resolveSchemaRevision(reg, schema.KindMeasure, "g", "m", seededLocatorRepoRev)
	assert.Equal(t, int64(0), got, "registry authoritative for kind but key absent should yield 0 so the bounded await runs")
}

func TestResolveSchemaRevision_NilRegistry_ReturnsFallback(t *testing.T) {
	got := resolveSchemaRevision(nil, schema.KindMeasure, "g", "m", seededLocatorRepoRev)
	assert.Equal(t, seededLocatorRepoRev, got)
}

func TestResolveQueryGateRevision_RegistryAuthoritative_LocatorPresent_KeyAbsent_PreservesExistence(t *testing.T) {
	repo := newFakeRevisionRepo()
	reg := registry.NewNodeRepoRegistry()
	reg.Register(schema.KindMeasure, repo)

	rev, found := resolveQueryGateRevision(reg, schema.KindMeasure, "g", "m", seededLocatorRepoRev, true)
	assert.Equal(t, int64(0), rev)
	assert.True(t, found, "found must mirror locator existence so the gate emits SCHEMA_NOT_APPLIED, not NOT_FOUND")
}

func TestSchemaRevisionRegistry_NotProvider_ReturnsNil(t *testing.T) {
	// metadata.Repo nil interface — the type assertion should fail and yield nil.
	var repo metadata.Repo
	assert.Nil(t, schemaRevisionRegistry(repo))
}
