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

package export

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const testSessionID = "aaaa"

func dayRule(num uint32) *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: num}
}

func stagedGroup(name string, catalog commonv1.Catalog) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  catalog,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: dayRule(1),
			Ttl:             dayRule(7),
			Stages: []*commonv1.LifecycleStage{{
				Name: "warm", ShardNum: 4, SegmentInterval: dayRule(1), Ttl: dayRule(30), NodeSelector: "tier=warm",
			}},
		},
	}
}

type plannerFixture struct {
	planner    *Planner
	backends   Backends
	clock      *fakeClock
	groupLists *atomic.Int32
}

// newTestPlanner wires a Planner over fake backends and mocked registries. The stream
// catalog holds "sw_record" (one part) and "sw_log" (one part); property holds "sw_prop".
// groupLists counts the registry enumerations so a test can prove a path did not list.
func newTestPlanner(t *testing.T, labels map[string]string, groups []*commonv1.Group, measures map[string][]*databasev1.Measure) plannerFixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	repo := metadata.NewMockRepo(ctrl)
	groupReg := schema.NewMockGroup(ctrl)
	measureReg := schema.NewMockMeasure(ctrl)
	repo.EXPECT().GroupRegistry().Return(groupReg).AnyTimes()
	repo.EXPECT().MeasureRegistry().Return(measureReg).AnyTimes()
	groupLists := &atomic.Int32{}
	groupReg.EXPECT().ListGroup(gomock.Any()).DoAndReturn(func(context.Context) ([]*commonv1.Group, error) {
		groupLists.Add(1)
		return groups, nil
	}).AnyTimes()
	measureReg.EXPECT().ListMeasure(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, opt schema.ListOpt) ([]*databasev1.Measure, error) {
			return measures[opt.Group], nil
		}).AnyTimes()

	backends, clock := newTestBackends(t, "sw_record", "sw_log")
	writeFile(t, filepath.Join(backends[commonv1.Catalog_CATALOG_PROPERTY].GetDataPath(), "sw_prop", "shard-0", "x"), []byte("p"))

	p := NewPlanner(repo, backends, labels, logger.GetLogger("export-test"))
	p.sessions.now = clock.now
	return plannerFixture{planner: p, backends: backends, clock: clock, groupLists: groupLists}
}

func defaultGroups() []*commonv1.Group {
	return []*commonv1.Group{
		stagedGroup("sw_record", commonv1.Catalog_CATALOG_STREAM),
		stagedGroup("sw_log", commonv1.Catalog_CATALOG_STREAM),
		{Metadata: &commonv1.Metadata{Name: "sw_prop"}, Catalog: commonv1.Catalog_CATALOG_PROPERTY},
	}
}

func collect(t *testing.T, p *Planner, req *transferv1.PlanRequest) []*transferv1.PlanResponse {
	t.Helper()
	frames, err := collectErr(p, req)
	if err != nil {
		t.Fatal(err)
	}
	return frames
}

func collectErr(p *Planner, req *transferv1.PlanRequest) ([]*transferv1.PlanResponse, error) {
	var frames []*transferv1.PlanResponse
	err := p.PlanInventory(context.Background(), req, func(r *transferv1.PlanResponse) error {
		frames = append(frames, r)
		return nil
	})
	return frames, err
}

// unitFrames keeps the `units` frames of a data node stream.
func unitFrames(frames []*transferv1.PlanResponse) []*transferv1.UnitFrame {
	var out []*transferv1.UnitFrame
	for _, f := range frames {
		if u := f.GetUnits(); u != nil {
			out = append(out, u)
		}
	}
	return out
}

// segmentGroup is the group of a segment unit.
func segmentGroup(u *transferv1.UnitInventory) string { return u.GetSegment().GetUnit().GetGroup() }

func TestPlanInventory_OneFramePerGroupAndSelectorFilter(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)

	all := collect(t, fx.planner, &transferv1.PlanRequest{})
	if len(all) != 3 {
		t.Fatalf("want one frame per group (3), got %d", len(all))
	}
	for _, f := range unitFrames(all) {
		if f.NodeId != "" {
			t.Fatalf("node_id is stamped by the liaison, node must leave it empty: %q", f.NodeId)
		}
		seen := map[string]struct{}{}
		for _, u := range f.Units {
			if u.GetProperty() != nil {
				seen[u.GetProperty().GetGroup()] = struct{}{}
				continue
			}
			seen[segmentGroup(u)] = struct{}{}
		}
		if len(seen) != 1 {
			t.Fatalf("a frame must not span groups: %v", seen)
		}
	}

	only := collect(t, fx.planner, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
		{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"sw_log"}},
	}})
	if len(only) != 1 || segmentGroup(only[0].GetUnits().Units[0]) != "sw_log" {
		t.Fatalf("selector must narrow to sw_log, got %+v", only)
	}

	catalogOnly := collect(t, fx.planner, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_PROPERTY}}})
	if len(catalogOnly) != 1 || catalogOnly[0].GetUnits().Units[0].GetProperty().GetGroup() != "sw_prop" {
		t.Fatalf("catalog-only selector must return the property group as a PropertyInventory, got %+v", catalogOnly)
	}
}

func TestPlanInventory_StageFollowsNodeLabels(t *testing.T) {
	groups := []*commonv1.Group{stagedGroup("sw_record", commonv1.Catalog_CATALOG_STREAM)}
	hot := collect(t, newTestPlanner(t, nil, groups, nil).planner, &transferv1.PlanRequest{})
	if hot[0].GetUnits().Stage != "" {
		t.Fatalf("unlabeled node is the default tier and must report an empty stage, got %q", hot[0].GetUnits().Stage)
	}
	warm := collect(t, newTestPlanner(t, map[string]string{"tier": "warm"}, groups, nil).planner, &transferv1.PlanRequest{})
	if warm[0].GetUnits().Stage != "warm" {
		t.Fatalf("labeled node must report the matched stage, got %q", warm[0].GetUnits().Stage)
	}
	cold := collect(t, newTestPlanner(t, map[string]string{"tier": "cold"}, groups, nil).planner, &transferv1.PlanRequest{})
	if cold[0].GetUnits().Stage != "" {
		t.Fatalf("labels matching no stage must report an empty stage, got %q", cold[0].GetUnits().Stage)
	}
}

func TestPlanInventory_IndexModeMeasureCarriesSegmentLevelOnly(t *testing.T) {
	measureGroup := func(name string) *commonv1.Group {
		return &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name}, Catalog: commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: stagedGroup("", commonv1.Catalog_CATALOG_MEASURE).ResourceOpts,
		}
	}
	groups := []*commonv1.Group{measureGroup("sw_idx"), measureGroup("sw_plain")}
	measures := map[string][]*databasev1.Measure{
		"sw_idx":   {{Metadata: &commonv1.Metadata{Name: "m", Group: "sw_idx"}, IndexMode: true}},
		"sw_plain": {{Metadata: &commonv1.Metadata{Name: "p", Group: "sw_plain"}}},
	}
	fx := newTestPlanner(t, nil, groups, measures)
	for _, g := range []string{"sw_idx", "sw_plain"} {
		seg := filepath.Join(fx.backends[commonv1.Catalog_CATALOG_MEASURE].GetDataPath(), g, "seg-20260928")
		writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
		buildIndex(t, filepath.Join(seg, sidxDirName), 2)
	}

	frames := collect(t, fx.planner, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE}}})
	if len(frames) != 2 {
		t.Fatalf("want one frame per measure group, got %d", len(frames))
	}
	docCounts := map[string]uint64{}
	for _, f := range frames {
		u := f.GetUnits().Units[0].GetSegment()
		if len(u.Unit.ShardIds) != 0 || u.SegmentLevel == nil || u.SegmentLevel.EstimatedBytes == 0 {
			t.Fatalf("a shardless unit must carry segment-level stats: %+v", u)
		}
		docCounts[u.Unit.Group] = u.SegmentLevel.DocCount
	}
	if docCounts["sw_idx"] != 2 || docCounts["sw_plain"] != 0 {
		t.Fatalf("only the index-mode group counts its series documents: %v", docCounts)
	}
}

// createReq builds the CreateSession PlanRequest a liaison forwards: id already generated.
func createReq(id string, preempt bool, selectors ...*transferv1.Selector) *transferv1.PlanRequest {
	return &transferv1.PlanRequest{
		Selectors: selectors,
		Session:   &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Id: id, Preempt: preempt}},
	}
}

// readReq builds the ReadSession PlanRequest driving the session named by id.
func readReq(id string, selectors ...*transferv1.Selector) *transferv1.PlanRequest {
	return &transferv1.PlanRequest{
		Selectors: selectors,
		Session:   &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{Id: id}},
	}
}

func TestPlanInventory_SessionModes(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	// CreateSession on an empty node: group frames read from the snapshot and nothing else
	// (no summary frame when nothing was preempted; the liaison announces the session id).
	frames := collect(t, fx.planner, createReq(testSessionID, false))
	if got := unitFrames(frames); len(got) != 3 || len(frames) != 3 {
		t.Fatalf("snapshot inventory must be exactly the three group frames, got %+v", frames)
	}
	for _, f := range frames {
		if f.GetUnits() == nil {
			t.Fatalf("a data node sends neither a created frame nor an empty summary: %+v", f)
		}
	}

	// Mutating the live directory must not change what the session reports.
	if err := os.RemoveAll(filepath.Join(fx.backends[commonv1.Catalog_CATALOG_STREAM].GetDataPath(), "sw_record")); err != nil {
		t.Fatal(err)
	}
	fx.clock.advance(time.Hour)
	again := collect(t, fx.planner, readReq(testSessionID))
	if got := unitFrames(again); len(got) != 3 {
		t.Fatalf("session plan must read the snapshot, got %d frames", len(got))
	}
	for catalog := range allCatalogs() {
		lease, readErr := readLease(mustSessionDir(t, fx.planner.sessions, catalog, testSessionID))
		if readErr != nil || lease.LastHeartbeatAt != fx.clock.now().UnixNano() ||
			lease.ExpiresAt != fx.clock.now().Add(defaultLease).UnixNano() {
			t.Fatalf("ReadSession must renew the %s lease: %+v %v", catalog, lease, readErr)
		}
	}

	// ACTION_HEARTBEAT: a done frame, lease rewritten, no group enumeration.
	fx.clock.advance(time.Minute)
	lists := fx.groupLists.Load()
	frame, err := fx.planner.Sessions(&transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: testSessionID})
	if err != nil || frame.GetDone() == nil {
		t.Fatalf("heartbeat must answer done, got %+v, %v", frame, err)
	}
	if fx.groupLists.Load() != lists {
		t.Fatal("heartbeat must not enumerate groups")
	}
	for catalog := range allCatalogs() {
		lease, readErr := readLease(mustSessionDir(t, fx.planner.sessions, catalog, testSessionID))
		if readErr != nil || lease.LastHeartbeatAt != fx.clock.now().UnixNano() {
			t.Fatalf("heartbeat must rewrite the %s lease: %+v %v", catalog, lease, readErr)
		}
	}

	// Live-directory mode reflects the removal.
	if live := unitFrames(collect(t, fx.planner, &transferv1.PlanRequest{})); len(live) != 2 {
		t.Fatalf("live plan must reflect the live directory, got %d frames", len(live))
	}
}

func TestPlanInventory_CreateSessionOnlySnapshotsSelectedCatalogs(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	frames := collect(t, fx.planner, createReq(testSessionID, false,
		&transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_STREAM}))
	if got := unitFrames(frames); len(got) != 2 {
		t.Fatalf("want the two stream groups, got %d", len(got))
	}
	lease, _ := fx.planner.sessions.probe()
	if lease == nil || len(lease.Catalogs) != 1 || lease.Catalogs[0] != commonv1.Catalog_CATALOG_STREAM {
		t.Fatalf("only the selected catalog must carry a session dir: %+v", lease)
	}
}

func TestPlanInventory_PreemptReportsOldSessions(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	_ = collect(t, fx.planner, createReq(testSessionID, false))
	_, err := collectErr(fx.planner, createReq("bbbb", false))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("want AlreadyExists, got %v", err)
	}
	frames := collect(t, fx.planner, createReq("bbbb", true))
	last := frames[len(frames)-1]
	if got := last.GetSummary().GetPreemptedSessionIds(); last.GetSummary() == nil || len(got) != 1 || got[0] != testSessionID {
		t.Fatalf("the summary frame must close the stream and list the preempted session: %+v", last)
	}
	if got := unitFrames(frames); len(got) != 3 || len(got) != len(frames)-1 {
		t.Fatalf("the unit frames must precede the summary, got %d of %d frames", len(got), len(frames))
	}
	// How long ago the occupant last heartbeat does not change the answer: create takes no
	// liveness decision, so even a long-silent occupant is refused without preempt.
	fx.clock.advance(time.Hour)
	_, err = collectErr(fx.planner, createReq("cccc", false))
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), "bbbb") {
		t.Fatalf("want AlreadyExists naming bbbb, got %v", err)
	}
}

func TestPlanInventory_PreemptReportsOldSessionsWhenTheWalkFails(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	_ = collect(t, fx.planner, createReq(testSessionID, false))
	// The new snapshot copies a corrupt part, so the walk fails after the preemption.
	meta := filepath.Join(fx.backends[commonv1.Catalog_CATALOG_STREAM].GetDataPath(),
		"sw_record", "seg-20260928", "shard-0", "0000000000000001", "metadata.json")
	writeFile(t, meta, []byte(`{broken`))
	frames, err := collectErr(fx.planner, createReq("bbbb", true))
	if status.Code(err) != codes.Internal {
		t.Fatalf("a corrupt part must answer Internal, got %v", err)
	}
	if len(frames) == 0 {
		t.Fatal("the preempted sessions must still be reported")
	}
	last := frames[len(frames)-1]
	if got := last.GetSummary().GetPreemptedSessionIds(); len(got) != 1 || got[0] != testSessionID {
		t.Fatalf("the summary frame must close the stream and list the preempted session: %+v", last)
	}
}

func TestPlanInventory_SessionModeRefusesCatalogOutsideSession(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	_ = collect(t, fx.planner, createReq(testSessionID, false,
		&transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_STREAM}))
	_, err := collectErr(fx.planner, readReq(testSessionID,
		&transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_PROPERTY, Groups: []string{"sw_prop"}}))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no snapshot for catalog CATALOG_PROPERTY") {
		t.Fatalf("a selector naming a catalog the session does not hold must be FailedPrecondition, got %v", err)
	}
	// Without selectors the missing catalog is simply not part of the session.
	if got := unitFrames(collect(t, fx.planner, readReq(testSessionID))); len(got) != 2 {
		t.Fatalf("want the two stream groups, got %d", len(got))
	}
}

func TestPlanInventory_SkipsGroupDirectoriesTheRegistryDoesNotKnow(t *testing.T) {
	// The registry knows only sw_record; the data directory also holds sw_log. Both modes read
	// the group directories of their root and keep only the registered groups.
	fx := newTestPlanner(t, map[string]string{"tier": "warm"}, []*commonv1.Group{stagedGroup("sw_record", commonv1.Catalog_CATALOG_STREAM)}, nil)
	streamOnly := &transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_STREAM}
	// Ordered: the read needs the session the create makes.
	for _, tc := range []struct {
		req  *transferv1.PlanRequest
		name string
	}{
		{name: "live", req: &transferv1.PlanRequest{Selectors: []*transferv1.Selector{streamOnly}}},
		{name: "create", req: createReq(testSessionID, false, streamOnly)},
		{name: "read", req: readReq(testSessionID, streamOnly)},
	} {
		frames := unitFrames(collect(t, fx.planner, tc.req))
		if len(frames) != 1 || segmentGroup(frames[0].Units[0]) != "sw_record" || frames[0].Stage != "warm" {
			t.Fatalf("%s: want only the registered group sw_record (warm), got %+v", tc.name, frames)
		}
	}
	named := unitFrames(collect(t, fx.planner, readReq(testSessionID,
		&transferv1.Selector{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"sw_log"}})))
	if len(named) != 0 {
		t.Fatalf("a selector naming an unregistered group finds nothing, got %+v", named)
	}
}

func TestHasIndexModeMeasure(t *testing.T) {
	idx := func(name string, indexMode bool) *databasev1.Measure {
		return &databasev1.Measure{Metadata: &commonv1.Metadata{Name: name, Group: "g"}, IndexMode: indexMode}
	}
	for name, tc := range map[string]struct {
		measures []*databasev1.Measure
		want     bool
	}{
		"none":  {nil, false},
		"all":   {[]*databasev1.Measure{idx("a", true), idx("b", true)}, true},
		"mixed": {[]*databasev1.Measure{idx("a", true), idx("b", false)}, true},
		"plain": {[]*databasev1.Measure{idx("a", false)}, false},
	} {
		fx := newTestPlanner(t, nil, nil, map[string][]*databasev1.Measure{"g": tc.measures})
		got, err := fx.planner.hasIndexModeMeasure(context.Background(), "g")
		if err != nil || got != tc.want {
			t.Fatalf("%s: hasIndexModeMeasure = %v, %v; want %v", name, got, err, tc.want)
		}
	}
}

func TestPlanGroup_MeasureWithoutSegmentsSkipsRegistry(t *testing.T) {
	// No MeasureRegistry expectation: any registry lookup fails the test.
	p := NewPlanner(metadata.NewMockRepo(gomock.NewController(t)), nil, nil, logger.GetLogger("export-test"))
	g := &commonv1.Group{Metadata: &commonv1.Metadata{Name: "m"}, Catalog: commonv1.Catalog_CATALOG_MEASURE}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "m", "seg-bak", "x"), []byte("x")) // a file and a non-seg dir are not segments
	writeFile(t, filepath.Join(root, "m", "x"), []byte("x"))
	for _, dir := range []string{root, filepath.Join(root, "missing")} {
		frame, err := p.planGroup(context.Background(), g, dir)
		if err != nil || frame != nil {
			t.Fatalf("a measure group without segments must yield no frame, got %v, %v", frame, err)
		}
	}
}

func TestPlanInventory_InvalidSessionRequests(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	for _, req := range []*transferv1.PlanRequest{
		createReq("", false), // liaison must supply the id
		readReq(""),          // needs the session id
	} {
		if _, err := collectErr(fx.planner, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%+v must be InvalidArgument, got %v", req, err)
		}
	}
	if _, err := collectErr(fx.planner, readReq("deadbeef")); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown session must be NotFound, got %v", err)
	}
}

func TestSessions_Actions(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	heartbeat := func(id string) *transferv1.SessionsRequest {
		return &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: id}
	}
	release := func(id string) *transferv1.SessionsRequest {
		return &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: id}
	}
	list := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST}

	for _, req := range []*transferv1.SessionsRequest{{}, heartbeat(""), release("")} {
		if _, err := fx.planner.Sessions(req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%+v must be InvalidArgument, got %v", req, err)
		}
	}
	if frame, err := fx.planner.Sessions(list); err != nil || frame.GetNone() == nil {
		t.Fatalf("an empty node lists none, got %+v, %v", frame, err)
	}
	// A session this node does not hold is an error frame, not a failure: the liaison relays
	// it so the client learns which nodes lost the session.
	frame, err := fx.planner.Sessions(heartbeat("deadbeef"))
	if err != nil || !strings.Contains(frame.GetError(), "deadbeef not found on this node") {
		t.Fatalf("heartbeat of an unknown session must answer an error frame, got %+v, %v", frame, err)
	}
	// Releasing what is not there succeeds and says so: none, not done.
	if frame, err = fx.planner.Sessions(release("deadbeef")); err != nil || frame.GetNone() == nil {
		t.Fatalf("release of a session the node does not hold must answer none, got %+v, %v", frame, err)
	}

	_ = collect(t, fx.planner, createReq(testSessionID, false))
	frame, err = fx.planner.Sessions(list)
	if err != nil || frame.GetSession().GetSessionId() != testSessionID {
		t.Fatalf("list must report the created session, got %+v, %v", frame, err)
	}
	// An expired lease fails the call: the client must stop its heartbeat.
	fx.clock.advance(defaultLease + time.Minute)
	if _, err = fx.planner.Sessions(heartbeat(testSessionID)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("heartbeat of an expired session must be FailedPrecondition, got %v", err)
	}
	if os.Geteuid() == 0 {
		return // root ignores the read-only directory the rest relies on
	}
	// A release that cannot remove the session answers an error frame, so a half-released
	// session stays visible instead of failing the fan-out.
	streamDir := mustSessionDir(t, fx.planner.sessions, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	if err = os.Chmod(streamDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(streamDir, 0o755) })
	if frame, err = fx.planner.Sessions(release(testSessionID)); err != nil || !strings.Contains(frame.GetError(), "release export session "+testSessionID) {
		t.Fatalf("a failed release must answer an error frame, got %+v, %v", frame, err)
	}
	if err = os.Chmod(streamDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if frame, err = fx.planner.Sessions(release(testSessionID)); err != nil || frame.GetDone() == nil {
		t.Fatalf("release of a held session must answer done, got %+v, %v", frame, err)
	}
	if frame, err = fx.planner.Sessions(release(testSessionID)); err != nil || frame.GetNone() == nil {
		t.Fatalf("a second release must answer none, got %+v, %v", frame, err)
	}
	if frame, err = fx.planner.Sessions(list); err != nil || frame.GetNone() == nil {
		t.Fatalf("the released session must be gone and listed as none, got %+v, %v", frame, err)
	}
}

func TestPlanInventory_CreateSessionWritesDefaultLease(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	_ = collect(t, fx.planner, createReq("aabb", false))
	streamDir := mustSessionDir(t, fx.planner.sessions, commonv1.Catalog_CATALOG_STREAM, "aabb")
	diskLease, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	// Creation writes expiresAt = creation time + defaultLease; nothing in the request can change it.
	if want := fx.clock.now().Add(defaultLease).UnixNano(); diskLease.ExpiresAt != want {
		t.Fatalf("on-disk lease ExpiresAt=%d, want creation time + default lease %d", diskLease.ExpiresAt, want)
	}
}

func TestPlanInventory_ReadSessionRemovedDuringWalkIsNotFound(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	collect(t, fx.planner, createReq(testSessionID, false))
	req := readReq(testSessionID)
	roots, _, err := fx.planner.resolveRoots(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// The session goes between resolving its roots and walking them.
	if _, err = fx.planner.sessions.release(testSessionID); err != nil {
		t.Fatal(err)
	}
	err = fx.planner.planRoots(context.Background(), req, roots, func(*transferv1.PlanResponse) error { return nil })
	if status.Code(err) != codes.NotFound {
		t.Fatalf("a session removed during the plan must answer NotFound, got %v", err)
	}
}

func TestPlanInventory_CorruptPartMetadataIsInternal(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	meta := filepath.Join(fx.backends[commonv1.Catalog_CATALOG_STREAM].GetDataPath(),
		"sw_record", "seg-20260928", "shard-0", "0000000000000001", "metadata.json")
	writeFile(t, meta, []byte(`{broken`))
	if _, err := collectErr(fx.planner, &transferv1.PlanRequest{}); status.Code(err) != codes.Internal {
		t.Fatalf("a corrupt part must answer Internal, got %v", err)
	}
}

func TestPlanInventory_CanceledIsCanceled(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fx.planner.PlanInventory(ctx, &transferv1.PlanRequest{}, func(*transferv1.PlanResponse) error { return nil })
	if status.Code(err) != codes.Canceled {
		t.Fatalf("a canceled plan must answer Canceled, got %v", err)
	}
}
