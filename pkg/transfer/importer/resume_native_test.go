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
	"bytes"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// storingTarget is a fakePartsLiaison that stores what it acknowledges: it counts every payload
// stored per node, and its Plan reports the stream rows of the stored parts (plus foreign rows).
type storingTarget struct {
	f *fakePartsLiaison
	// behave, when set, may decide an answer instead of storing on every node; n counts the
	// payloads of its kind. It returns the nodes to store on and the answer, or all nil.
	behave  func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error)
	stored  map[string]int
	rows    map[string]int64
	foreign map[string]int64
	kinds   map[nativefmt.Kind]int
	mu      sync.Mutex
}

func newStoringTarget(t *testing.T) (*storingTarget, *cluster.Liaison) {
	t.Helper()
	s := &storingTarget{stored: map[string]int{}, rows: map[string]int64{}, foreign: map[string]int64{}, kinds: map[nativefmt.Kind]int{}}
	s.f = &fakePartsLiaison{plan: func(int) *Counts {
		s.mu.Lock()
		defer s.mu.Unlock()
		return streamCounts(s.rows["d1"]+s.foreign["d1"], s.rows["d2"]+s.foreign["d2"])
	}}
	s.f.answer = func(_ int, p *recordedPayload) (*transferv1.ImportPartsResponse, error) {
		s.mu.Lock()
		n := s.kinds[p.init.GetKind()]
		s.kinds[p.init.GetKind()]++
		behave := s.behave
		s.mu.Unlock()
		nodes := p.init.GetRetryNodes()
		if len(nodes) == 0 {
			nodes = []string{"d1", "d2"}
		}
		store, resp := nodes, okTargets(nodes...)
		var err error
		if behave != nil {
			if st, r, e := behave(n, p); st != nil || r != nil || e != nil {
				store, resp, err = st, r, e
			}
		}
		s.store(p, store)
		return resp, err
	}
	return s, s.f.serve(t)
}

func (s *storingTarget) store(p *recordedPayload, nodes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range nodes {
		s.stored[payloadName(p)+"@"+n]++
		if p.init.GetKind() == nativefmt.KindPart {
			s.rows[n] += int64(p.init.GetParts()[0].GetTotalCount())
		}
	}
}

func (s *storingTarget) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.stored)
}

func (s *storingTarget) set(behave func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error), foreign map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.behave, s.foreign = behave, foreign
}

func codeOf(err error) int {
	if err == nil {
		return 0
	}
	return exitcode.For(err)
}

func payloadName(p *recordedPayload) string {
	if p.init.GetKind() == nativefmt.KindPart {
		return fmt.Sprintf("PART %d", p.init.GetParts()[0].GetId())
	}
	return kindName(p.init.GetKind())
}

func payloadCount(f *fakePartsLiaison) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.payloads)
}

// sentKinds lists the payloads the fake received from the n-th on, by name and retry nodes.
func sentKinds(f *fakePartsLiaison, from int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, p := range f.payloads[from:] {
		out = append(out, fmt.Sprintf("%s%v", payloadName(p), p.init.GetRetryNodes()))
	}
	return out
}

// everyPayloadOnce is what one uninterrupted import of nativeArtifact stores.
var everyPayloadOnce = map[string]int{
	"SERIES_INDEX@d1": 1, "SERIES_INDEX@d2": 1, "PART 1@d1": 1, "PART 1@d2": 1, "ELEMENT_INDEX@d1": 1, "ELEMENT_INDEX@d2": 1,
}

// crashOn makes the first payload of kind store on the nodes store and then cuts the run off
// before its answer reaches the import, as a crash or Ctrl-C at that moment does.
func crashOn(s *storingTarget, kind nativefmt.Kind, cancel context.CancelFunc, store ...string) {
	if store == nil {
		store = []string{}
	}
	s.set(func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != kind || n > 0 {
			return nil, nil, nil
		}
		s.store(p, store)
		cancel()
		time.Sleep(50 * time.Millisecond)
		return []string{}, nil, status.Error(codes.Unavailable, "the import is gone")
	}, nil)
}

// nativeRun runs the push of a native import as RunImport does, recording the progress in path
// and resuming what it records when resumed.
func nativeRun(ctx context.Context, t *testing.T, li *cluster.Liaison, path string, resumed bool, policy config.DuplicationPolicy) (string, error) {
	t.Helper()
	art, entries := nativeArtifact(t) //nolint:contextcheck // writing a test fixture takes no context
	return nativeRunOf(ctx, t, li, path, resumed, policy, art, entries)
}

// nativeRunOf is nativeRun of another artifact.
func nativeRunOf(ctx context.Context, t *testing.T, li *cluster.Liaison, path string, resumed bool, policy config.DuplicationPolicy,
	art *Artifact, entries []Entry,
) (string, error) {
	t.Helper()
	return nativeRunWith(ctx, t, li, path, resumed, art, entries, func(o *config.Import) { o.DuplicationPolicy = policy })
}

// nativeRunWith is nativeRunOf with the import settings tuned by tune.
func nativeRunWith(ctx context.Context, t *testing.T, li *cluster.Liaison, path string, resumed bool, art *Artifact, entries []Entry,
	tune func(*config.Import),
) (string, error) {
	t.Helper()
	baseline := streamCounts(0, 0)
	var state *progressState
	if resumed {
		var err error
		state, err = loadProgress(path, testBindings())
		require.NoError(t, err)
	} else {
		state = newProgress(art, testBindings())
		digest, err := writePlan(path, nil, baseline)
		require.NoError(t, err)
		at := time.Now()
		state.PlanDigest, state.BaselineAt, state.baseline = digest, &at, baseline
	}
	track, err := startTracker(context.WithoutCancel(ctx), path, state, 64, nil)
	require.NoError(t, err)
	rec := newReconciler(baseline, func(ctx context.Context) (*Counts, error) { return Snapshot(ctx, li.Conn, nil) })
	rec.interval = time.Millisecond
	opts := nativePushOptions().Import
	tune(&opts)
	// An import that sets no policy has the default, as bydbctl resolves it.
	var source string
	if opts.DuplicationPolicy == "" {
		opts.DuplicationPolicy, source = config.DuplicationWarn, "default"
	}
	var log bytes.Buffer
	_, _, err = pushWithProgress(ctx, Env{Log: &log, Liaisons: &cluster.Liaisons{All: []*cluster.Liaison{li}}, DuplicationSource: source}, art, opts, nil, entries,
		progressRun{state: state, track: track, rec: rec, resumed: resumed})
	require.NoError(t, track.close())
	return log.String(), err
}

func TestResumeNative_InterruptedPushStoresEveryPayloadOnce(t *testing.T) {
	shortenPartRetries(t)
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	ctx, cancel := context.WithCancel(context.Background())
	// The element index is cut off after it was sent: in flight at the interruption.
	crashOn(s, nativefmt.KindElementIndex, cancel)
	_, err := nativeRun(ctx, t, li, path, false, "")
	require.Error(t, err)
	state := readState(t, path)
	require.Len(t, state.Inflight, 1)
	for _, in := range state.Inflight {
		assert.Equal(t, "ELEMENT_INDEX", in.Kind)
	}
	s.set(nil, nil)
	sent := payloadCount(s.f)

	log, err := nativeRun(context.Background(), t, li, path, true, "")
	require.NoError(t, err, log)
	assert.Equal(t, everyPayloadOnce, s.snapshot())
	assert.Equal(t, []string{"ELEMENT_INDEX[d1 d2]"}, sentKinds(s.f, sent), "the series index and the part are done; the element index is pushed again")
	assert.Contains(t, log, "WARN "+streamShardKey+" ELEMENT_INDEX of n1 on d1,d2 may have landed already; "+
		"it is pushed again (resumeDuplicationPolicy warn, default)")
	state = readState(t, path)
	assert.True(t, state.Done[streamShardKey].Complete)
	assert.Empty(t, state.Inflight)
}

func TestResumeNative_InflightStreamPartIsReconciled(t *testing.T) {
	shortenPartRetries(t)
	for _, tc := range []struct {
		foreign map[string]int64
		want    map[string]int
		name    string
		policy  config.DuplicationPolicy
		log     string
		store   []string
		sent    []string
		code    int
	}{
		{
			name: "landed", store: []string{"d1", "d2"}, want: everyPayloadOnce, sent: []string{"ELEMENT_INDEX[]"},
			log: "reconcile " + streamShardKey + " part 1 of n1 on d2: landed (0→4, part 4 rows)",
		},
		{
			name: "not landed", want: everyPayloadOnce, sent: []string{"PART 1[d1 d2]", "ELEMENT_INDEX[]"},
			log: "reconcile " + streamShardKey + " part 1 of n1 on d1: not landed (0→0, part 4 rows)",
		},
		{
			name: "half landed", store: []string{"d1"}, want: everyPayloadOnce, sent: []string{"PART 1[d2]", "ELEMENT_INDEX[]"},
			log: "on d2: not landed",
		},
		{
			name: "undecidable, block", store: []string{"d1", "d2"}, foreign: map[string]int64{"d2": 2}, policy: config.DuplicationBlock, code: exitcode.ExitHalted,
			want: map[string]int{"SERIES_INDEX@d1": 1, "SERIES_INDEX@d2": 1, "PART 1@d1": 1, "PART 1@d2": 1},
			log:  "on d2: undecidable (0→6, part 4 rows): the rows grew by 6, neither 0 nor the part's 4",
		},
		{
			name: "undecidable, warn", store: []string{"d1", "d2"}, foreign: map[string]int64{"d2": 2}, policy: config.DuplicationWarn,
			want: map[string]int{"SERIES_INDEX@d1": 1, "SERIES_INDEX@d2": 1, "PART 1@d1": 1, "PART 1@d2": 2, "ELEMENT_INDEX@d1": 1, "ELEMENT_INDEX@d2": 1},
			sent: []string{"PART 1[d2]", "ELEMENT_INDEX[]"}, log: "WARN " + streamShardKey + " part 1 of n1 on d2 may have landed already",
		},
		{
			name: "undecidable, skip", store: []string{"d1", "d2"}, foreign: map[string]int64{"d2": 2}, policy: config.DuplicationSkip,
			want: everyPayloadOnce, sent: []string{"ELEMENT_INDEX[]"}, log: "it is left out (resumeDuplicationPolicy skip, --resume-duplication-policy)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, li := newStoringTarget(t)
			path := filepath.Join(t.TempDir(), "progress.json")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			crashOn(s, nativefmt.KindPart, cancel, tc.store...)
			_, err := nativeRun(ctx, t, li, path, false, "")
			require.Error(t, err)
			s.set(nil, tc.foreign)
			sent := payloadCount(s.f)

			log, err := nativeRun(context.Background(), t, li, path, true, tc.policy)
			assert.Equal(t, tc.code, codeOf(err), "%v\n%s", err, log)
			assert.Contains(t, log, tc.log)
			assert.Equal(t, tc.want, s.snapshot())
			assert.Equal(t, tc.sent, sentKinds(s.f, sent))
		})
	}
}

func TestResumeNative_MissingNodeIsRetriedAlone(t *testing.T) {
	shortenPartRetries(t)
	attempts := partRetryAttempts
	partRetryAttempts = 1
	t.Cleanup(func() { partRetryAttempts = attempts })
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	s.set(func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != nativefmt.KindPart || n > 0 {
			return nil, nil, nil
		}
		resp := okTargets("d1", "d2")
		resp.Targets[1].Success, resp.Targets[1].Error = false, "disk full"
		return []string{"d1"}, resp, nil
	}, nil)
	_, err := nativeRun(context.Background(), t, li, path, false, "")
	require.Error(t, err)
	part := readState(t, path).Done[streamShardKey].Parts["n1/1"]
	require.NotNil(t, part)
	assert.Equal(t, []string{"d1"}, part.Nodes)
	assert.Equal(t, []string{"d2"}, part.Missing)
	s.set(nil, nil)
	sent := payloadCount(s.f)

	log, err := nativeRun(context.Background(), t, li, path, true, config.DuplicationBlock)
	require.NoError(t, err, log)
	assert.Equal(t, []string{"PART 1[d2]", "ELEMENT_INDEX[]"}, sentKinds(s.f, sent), "only the node it missed, and no policy is involved")
	assert.Equal(t, everyPayloadOnce, s.snapshot())
}

func TestResumeNative_AmbiguousElementIndexFollowsThePolicy(t *testing.T) {
	shortenPartRetries(t)
	for _, tc := range []struct {
		policy config.DuplicationPolicy
		sent   []string
		code   int
	}{
		{policy: config.DuplicationBlock, code: exitcode.ExitHalted},
		{policy: config.DuplicationPush, sent: []string{"ELEMENT_INDEX[d2]"}},
		// verify does not count element index documents, so skip is refused.
		{policy: config.DuplicationSkip, code: exitcode.ExitHalted},
	} {
		s, li := newStoringTarget(t)
		path := filepath.Join(t.TempDir(), "progress.json")
		s.set(func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error) {
			if p.init.GetKind() != nativefmt.KindElementIndex || n > 0 {
				return nil, nil, nil
			}
			resp := okTargets("d1", "d2")
			resp.Targets[1].Success, resp.Targets[1].Error, resp.Targets[1].CompletionSent = false, "stream reset", true
			return []string{"d1"}, resp, nil
		}, nil)
		_, err := nativeRun(context.Background(), t, li, path, false, "")
		require.Error(t, err)
		assert.Equal(t, []string{"d2"}, readState(t, path).Done[streamShardKey].Blobs["n1/ELEMENT_INDEX"].CompletionSent)
		s.set(nil, nil)
		sent := payloadCount(s.f)

		log, err := nativeRun(context.Background(), t, li, path, true, tc.policy)
		assert.Equal(t, tc.code, codeOf(err), "%s: %v\n%s", tc.policy, err, log)
		assert.Equal(t, tc.sent, sentKinds(s.f, sent), tc.policy)
		if tc.policy != config.DuplicationPush {
			assert.Contains(t, log, streamShardKey+" ELEMENT_INDEX of n1 on d2", tc.policy)
		} else {
			assert.Equal(t, everyPayloadOnce, s.snapshot(), "pushed again to the node only")
		}
	}
}

func TestResumeNative_InflightIdempotentPayloadsArePushedAgainWithoutThePolicy(t *testing.T) {
	shortenPartRetries(t)
	// A series index keeps the first copy of a document: pushed again whatever the policy.
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	crashOn(s, nativefmt.KindSeriesIndex, cancel, "d1", "d2")
	_, err := nativeRun(ctx, t, li, path, false, "")
	require.Error(t, err)
	s.set(nil, nil)
	sent := payloadCount(s.f)
	log, err := nativeRun(context.Background(), t, li, path, true, config.DuplicationBlock)
	require.NoError(t, err, log)
	assert.Equal(t, []string{"SERIES_INDEX[d1 d2]", "PART 1[]", "ELEMENT_INDEX[]"}, sentKinds(s.f, sent))

	// A measure part converges on its versions: pushed again whatever the policy, not reconciled.
	art, e := shardArtifact(t, measure, "mg", 4, []string{"d1", "d2"},
		member{"0000000000000001/metadata.json", coreMeta(4)},
		member{"0000000000000001/primary.bin", "PRIMARY"},
		member{"0000000000000001/meta.bin", "META"},
		member{"0000000000000001/timestamps.bin", "TS"},
		member{"0000000000000001/fv.bin", "FV"},
		member{"0000000000000001.snp", `["0000000000000001"]`},
	)
	s, li = newStoringTarget(t)
	path = filepath.Join(t.TempDir(), "progress.json")
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	crashOn(s, nativefmt.KindPart, cancel2, "d1", "d2")
	_, err = nativeRunOf(ctx2, t, li, path, false, "", art, []Entry{e})
	require.Error(t, err)
	s.set(nil, nil)
	sent = payloadCount(s.f)
	s.f.mu.Lock()
	plans := s.f.planCalls
	s.f.mu.Unlock()
	log, err = nativeRunOf(context.Background(), t, li, path, true, config.DuplicationBlock, art, []Entry{e})
	require.NoError(t, err, log)
	assert.Equal(t, []string{"PART 1[d1 d2]"}, sentKinds(s.f, sent))
	s.f.mu.Lock()
	assert.Equal(t, plans, s.f.planCalls, "a measure part is not reconciled")
	s.f.mu.Unlock()
	assert.NotContains(t, log, "reconcile ")
}

func TestResumeNative_PartlyDecidedReconciliationIsRecorded(t *testing.T) {
	shortenPartRetries(t)
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	s.set(func(n int, p *recordedPayload) ([]string, *transferv1.ImportPartsResponse, error) {
		if p.init.GetKind() != nativefmt.KindPart || n > 0 {
			return nil, nil, nil
		}
		resp := okTargets("d1", "d2")
		for _, tg := range resp.Targets {
			tg.Success, tg.Error, tg.CompletionSent = false, "stream reset", true
		}
		return []string{"d1"}, resp, nil
	}, map[string]int64{"d2": 2})
	log, err := nativeRun(context.Background(), t, li, path, false, "")
	require.Error(t, err, log)
	part := readState(t, path).Done[streamShardKey].Parts["n1/1"]
	require.NotNil(t, part)
	assert.Equal(t, []string{"d1"}, part.Nodes, "the node decided as landed is recorded")
	assert.Equal(t, []string{"d2"}, part.CompletionSent)
}

func TestResumeNative_CanceledBeforeTheCompletionIsMissingNotAmbiguous(t *testing.T) {
	shortenPartRetries(t)
	s, li := newStoringTarget(t)
	path := filepath.Join(t.TempDir(), "progress.json")
	art, entries := nativeArtifact(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 1 byte/s with 4-byte chunks: the second chunk waits, and the run is canceled once the
	// first one arrived.
	var once sync.Once
	s.f.onChunk = func() { once.Do(cancel) }
	_, err := nativeRunWith(ctx, t, li, path, false, art, entries, func(o *config.Import) { o.PartRateLimit = 1 })
	require.Error(t, err)
	state := readState(t, path)
	assert.Empty(t, state.Inflight, "a payload whose completion was not sent is not in flight")
	var missing []string
	for _, r := range state.Done {
		for _, p := range r.Blobs {
			missing = append(missing, p.Missing...)
		}
		for _, p := range r.Parts {
			missing = append(missing, p.Missing...)
		}
	}
	assert.ElementsMatch(t, []string{"d1", "d2"}, missing, "the canceled payload misses both its targets")

	log, err := nativeRun(context.Background(), t, li, path, true, config.DuplicationBlock)
	require.NoError(t, err, log)
	assert.NotContains(t, log, "may have landed")
	assert.Equal(t, everyPayloadOnce, s.snapshot())
}
