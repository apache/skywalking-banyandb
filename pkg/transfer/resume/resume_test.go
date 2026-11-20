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

package resume

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

type doc struct {
	Header
	Items []int `json:"items"`
}

func requireCode(t *testing.T, err error, code int, contains string) {
	t.Helper()
	var exitErr *exitcode.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, code, exitErr.Code)
	assert.Contains(t, err.Error(), contains)
}

func TestWriteFile_IsAtomicAndPrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".p.json")
	require.NoError(t, WriteFile(path, &doc{Header: Header{Kind: "k", Version: 1}, Items: []int{1}}))
	require.NoError(t, WriteFile(path, &doc{Header: Header{Kind: "k", Version: 1}, Items: []int{1, 2}}))
	info, err := os.Stat(path)
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		// Windows has no permission bits: a writable file reads as 0666.
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the temporary copy is renamed away")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "{\n  \"kind\": \"k\",\n  \"version\": 1,\n  \"items\": [\n    1,\n    2\n  ]\n}\n", string(data), "indented JSON")
	var got doc
	require.NoError(t, Read(path, "k", 1, &got))
	assert.Equal(t, []int{1, 2}, got.Items)

	if os.Getuid() != 0 && runtime.GOOS != "windows" {
		// A failed write leaves the previous file intact. A read-only directory does not stop
		// a file from being created on Windows, nor for root.
		require.NoError(t, os.Chmod(dir, 0o500))
		require.Error(t, WriteFile(path, &doc{Header: Header{Kind: "k", Version: 1}, Items: []int{9}}))
		require.NoError(t, os.Chmod(dir, 0o700))
		require.NoError(t, Read(path, "k", 1, &got))
		assert.Equal(t, []int{1, 2}, got.Items)
	}

	require.NoError(t, Remove(path))
	assert.False(t, Exists(path))
	require.NoError(t, Remove(path), "removing what is gone is fine")
}

func TestRemove_TakesTempAndSideFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".p.json")
	require.NoError(t, WriteFile(path, &doc{Items: []int{1}}))
	digest, err := WriteSide(SidePath(path, "plan"), &doc{Items: []int{7}})
	require.NoError(t, err)
	for _, leftover := range []string{"..p.json.123.tmp", "..p-plan.json.9.tmp", "other.tmp", "..p.json.tmp.keep"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, leftover), nil, 0o600))
	}
	assert.True(t, IsTemp(path, "..p.json.123.tmp"))
	assert.False(t, IsTemp(path, "..p.json.tmp"), "a temporary file has a unique part")
	var side doc
	require.NoError(t, ReadSide(SidePath(path, "plan"), digest, &side))
	assert.Equal(t, []int{7}, side.Items)

	require.NoError(t, Remove(path, "plan"))
	var left []string
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		left = append(left, e.Name())
	}
	assert.ElementsMatch(t, []string{"other.tmp", "..p.json.tmp.keep"}, left)
}

func TestReadSide_RefusesAnotherFileWithExit6(t *testing.T) {
	side := filepath.Join(t.TempDir(), "p-plan.json")
	digest, err := WriteSide(side, &doc{Items: []int{1}})
	require.NoError(t, err)
	var got doc
	requireCode(t, ReadSide(side, "sha256:00", &got), exitcode.ExitResume, "is not the one")
	require.NoError(t, os.WriteFile(side, []byte(`{"items":[2]}`+"\n"), 0o600))
	requireCode(t, ReadSide(side, digest, &got), exitcode.ExitResume, "is not the one")
	require.NoError(t, os.Remove(side))
	requireCode(t, ReadSide(side, digest, &got), exitcode.ExitResume, "side file")
}

func TestReadSide_NamesAnEmptyOrCutShortFile(t *testing.T) {
	side := filepath.Join(t.TempDir(), "p-plan.json")
	digest, err := WriteSide(side, &doc{Items: []int{1, 2, 3}})
	require.NoError(t, err)
	full, err := os.ReadFile(side)
	require.NoError(t, err)
	var got doc
	for name, content := range map[string][]byte{"empty": nil, "cut short": full[:len(full)/2]} {
		require.NoError(t, os.WriteFile(side, content, 0o600), name)
		err = ReadSide(side, digest, &got)
		requireCode(t, err, exitcode.ExitResume, "empty or incomplete")
		assert.NotContains(t, err.Error(), "is not the one", name)
	}
}

func TestCheckSideCollision(t *testing.T) {
	require.NoError(t, CheckSideCollision(filepath.Join("d", ".export-progress.json")))
	require.NoError(t, CheckSideCollision("p"))
	for _, p := range []string{"p-plan.json", filepath.Join("d", "x-cursor.json")} {
		err := CheckSideCollision(p)
		require.Error(t, err, p)
		assert.Contains(t, err.Error(), p)
	}
}

func TestRead_RefusesUnusableFilesWithExit6(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"torn":          `{"kind":"k",`,
		"other kind":    `{"kind":"import-progress","version":1}`,
		"other version": `{"kind":"k","version":2}`,
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "-"))
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		var got doc
		requireCode(t, Read(path, "k", 1, &got), exitcode.ExitResume, "progress file")
	}
	var got doc
	requireCode(t, Read(filepath.Join(dir, "missing"), "k", 1, &got), exitcode.ExitResume, "missing")
	// The kind and version are named whether or not the rest of the file decodes.
	for body, want := range map[string]string{
		`{"kind":"x","version":1,"items":[1]}`:  `is a "x" version 1 file`,
		`{"kind":"x","version":1,"items":"no"}`: `is a "x" version 1 file`,
		`{"kind":"k","version":1,"items":"no"}`: "is unusable",
	} {
		path := filepath.Join(dir, "body")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		requireCode(t, Read(path, "k", 1, &got), exitcode.ExitResume, want)
	}
}

func TestPersister_AppliesInOrderAndSyncs(t *testing.T) {
	var mu sync.Mutex
	var written [][]int
	state := &doc{}
	p := NewPersister(context.Background(), state, func(d *doc) error {
		mu.Lock()
		defer mu.Unlock()
		written = append(written, append([]int(nil), d.Items...))
		return nil
	}, nil)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test workers feeding the persister
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				p.Update(func(d *doc) { d.Items = append(d.Items, i) })
			}
		}()
	}
	wg.Wait()
	require.NoError(t, p.Sync(func(d *doc) { d.Items = append(d.Items, -1) }))
	mu.Lock()
	last := written[len(written)-1]
	mu.Unlock()
	assert.Len(t, last, 101, "Sync returns once every change queued before it is written")
	assert.Equal(t, -1, last[100])
	require.NoError(t, p.Close())
	require.NoError(t, p.Close(), "Close is idempotent")
}

func TestPersister_FirstWriteErrorIsSticky(t *testing.T) {
	writes := 0
	var reported []error
	p := NewPersister(context.Background(), &doc{}, func(*doc) error {
		writes++
		return errors.New("disk full")
	}, func(err error) { reported = append(reported, err) })
	require.EqualError(t, p.Sync(nil), "disk full")
	p.Update(func(d *doc) { d.Items = append(d.Items, 1) })
	require.EqualError(t, p.Sync(nil), "disk full")
	p.UpdateLazy(func(d *doc) { d.Items = append(d.Items, 2) })
	require.EqualError(t, p.Close(), "disk full")
	assert.Equal(t, 1, writes, "nothing is written after the first failure, at Close neither")
	assert.Len(t, reported, 1)
}

func TestDigests(t *testing.T) {
	assert.Equal(t, DigestSet([]string{"b", "a", "a"}), DigestSet([]string{"a", "b"}))
	assert.NotEqual(t, DigestSet([]string{"ab", "c"}), DigestSet([]string{"a", "bc"}))
	assert.NotEqual(t, DigestSet([]string{"a\nb"}), DigestSet([]string{"a", "b"}))
	assert.NotEqual(t, DigestOptions(map[string]string{"a=b": "c"}), DigestOptions(map[string]string{"a": "b=c"}))
	assert.Equal(t, DigestOptions(map[string]string{"x": "1", "y": "2"}), DigestOptions(map[string]string{"y": "2", "x": "1"}))
	assert.True(t, strings.HasPrefix(DigestBytes([]byte("m")), "sha256:"))
	diff := DiffBindings([]Binding{
		{Name: "same", Recorded: "a", Current: "a"},
		{Name: "optionsDigest", Recorded: "a", Current: "b", Hint: "format, maxFileSize"},
	})
	assert.Equal(t, "  optionsDigest differs: recorded a, now b (format, maxFileSize)\n", diff)
	assert.Empty(t, DiffBindings([]Binding{{Name: "x", Recorded: "1", Current: "1"}}))
	assert.Equal(t, []string{"a: recorded 1, now 2", "b: recorded (none), now \"\"", "c: recorded x, now (none)"},
		OptionDiffs(map[string]string{"a": "1", "c": "x", "same": "s"}, map[string]string{"a": "2", "b": "", "same": "s"}, nil, nil))
	assert.Nil(t, OptionDiffs(map[string]string{"a": "1"}, map[string]string{"a": "1"}, nil, nil))
	kib := func(_, v string) string { return v + " KiB" }
	assert.Equal(t, []string{"a: recorded 1 KiB (default), now 2 KiB"},
		OptionDiffs(map[string]string{"a": "1"}, map[string]string{"a": "2"}, map[string]string{"a": "1"}, kib))
	assert.Equal(t, []string{"recorded only: a", "now only: d"}, SetDiffs([]string{"b", "a", "a"}, []string{"d", "b"}))
	assert.Nil(t, SetDiffs([]string{"a", "b"}, []string{"b", "a", "a"}), "order and duplicates do not count")
	assert.Equal(t, []string{"a", "c"}, Missing([]string{"c", "a", "a", "b"}, []string{"b"}))
	assert.Nil(t, Missing(nil, []string{"b"}))
}

func TestSummary_Render(t *testing.T) {
	localZone(t, time.UTC)
	updated := time.Date(2026, 11, 12, 8, 14, 3, 0, time.UTC)
	s := Summary{
		Path: "./backup-20260914/.export-progress.json", UpdatedAt: updated, Now: updated.Add(41 * time.Minute),
		Session: "9f2a0c", Command: "export", Noun: "unit(s)",
		Done: 863, Total: 1440, DoneFiles: 5212, DoneBytes: 504 << 30, LeftBytes: 3376 << 27, LeftShards: 96,
		Rows: []Row{
			{Catalog: "stream", Group: "sw_record", Stage: "warm", Done: 743, Total: 1272, DoneFiles: 4000, DoneBytes: 4857 << 27},
			{Catalog: "measure", Group: "sw_metric", Stage: "hot", Done: 120, Total: 168, DoneFiles: 1212, DoneBytes: 183 << 27},
			{Catalog: "trace", Group: "sw_trace", Stage: "hot", Total: 5},
		},
	}
	var b bytes.Buffer
	require.NoError(t, s.Render(&b))
	assert.Equal(t, `RESUME   ./backup-20260914/.export-progress.json  (last updated 2026-11-12T08:14:03Z, 41m ago)
         session 9f2a0c (export)
DONE     2 group(s), 863 unit(s), 5212 file(s), 504.0 GiB
CATALOG  GROUP      STAGE  UNITS  FILES  SIZE
measure  sw_metric  hot    120    1212   22.9 GiB
stream   sw_record  warm   743    4000   607.1 GiB

LEFT     3 group(s), 577 unit(s), 96 shard(s), est. 422.0 GiB
`, b.String())
}

// writeLog records every Write, as the terminal's progress writer sees them.
type writeLog struct{ writes []string }

func (l *writeLog) Write(b []byte) (int, error) {
	l.writes = append(l.writes, string(b))
	return len(b), nil
}

func TestSummary_RenderWritesTheTableInOneWrite(t *testing.T) {
	updated := time.Date(2026, 11, 12, 8, 14, 3, 0, time.UTC)
	s := Summary{
		Path: "p.json", UpdatedAt: updated, Now: updated, Noun: "unit(s)", Done: 2, Total: 3,
		Rows: []Row{
			{Catalog: "stream", Group: "sw_record", Stage: "warm", Done: 1, Total: 2, DoneFiles: 1, DoneBytes: 10},
			{Catalog: "measure", Group: "sw_metric", Stage: "hot", Done: 1, Total: 1, DoneFiles: 1, DoneBytes: 20},
		},
	}
	var l writeLog
	require.NoError(t, s.Render(&l))
	var table []string
	for _, w := range l.writes {
		if strings.Contains(w, "sw_record") || strings.Contains(w, "sw_metric") {
			table = append(table, w)
		}
	}
	require.Len(t, table, 1, "a writer that prefixes every Write with a carriage return must not see the table cell by cell")
	assert.Contains(t, table[0], "CATALOG")
}

func TestSummary_RenderWithLeftUnknown(t *testing.T) {
	localZone(t, time.UTC)
	updated := time.Date(2026, 11, 12, 8, 14, 3, 0, time.UTC)
	s := Summary{
		Path: "p.json", UpdatedAt: updated, Now: updated, Session: "9f2a0c", Command: "export", Noun: "unit(s)",
		LeftUnknown: "unknown: no plan is recorded yet",
	}
	var b bytes.Buffer
	require.NoError(t, s.Render(&b))
	assert.Equal(t, `RESUME   p.json  (last updated 2026-11-12T08:14:03Z, 0s ago)
         session 9f2a0c (export)
DONE     0 unit(s), 0 file(s), 0 B
LEFT     unknown: no plan is recorded yet
`, b.String())
}

// localZone makes the time zone of the operator zone for the test.
func localZone(t *testing.T, zone *time.Location) {
	t.Helper()
	prev := time.Local
	time.Local = zone
	t.Cleanup(func() { time.Local = prev })
}

func TestSummary_RenderShowsLocalTime(t *testing.T) {
	localZone(t, time.FixedZone("T", 9*3600))
	updated := time.Date(2026, 11, 12, 8, 14, 3, 0, time.UTC)
	s := Summary{Path: "p.json", UpdatedAt: updated, Now: updated, Noun: "unit(s)"}
	var b bytes.Buffer
	require.NoError(t, s.Render(&b))
	assert.Contains(t, b.String(), "last updated 2026-11-12T17:14:03+09:00")
}

func TestSummary_RenderFinished(t *testing.T) {
	localZone(t, time.UTC)
	updated := time.Date(2026, 11, 12, 8, 14, 3, 0, time.UTC)
	finished := updated.Add(-time.Minute)
	s := Summary{
		Path: "p.json", UpdatedAt: updated, Now: updated.Add(2 * time.Hour), Finished: &finished, Noun: "work item(s)",
		Done: 2, Total: 2, DoneFiles: 3, DoneBytes: 2048, Extra: []string{"AMBIGUOUS 1 payload(s) may have landed already:"},
		Rows: []Row{{Catalog: "measure", Group: "sw_metric", Stage: "hot", Done: 2, Total: 2, DoneFiles: 3, DoneBytes: 2048}},
	}
	var b bytes.Buffer
	require.NoError(t, s.Render(&b))
	assert.Equal(t, `FINISHED p.json  (finished at 2026-11-12T08:13:03Z, 2h1m ago)
DONE     1 group(s), 2 work item(s), 3 file(s), 2.0 KiB
CATALOG  GROUP      STAGE  ITEMS  FILES  SIZE
measure  sw_metric  hot    2      3      2.0 KiB
`, b.String())
}

func TestAge(t *testing.T) {
	for _, c := range []struct {
		want string
		d    time.Duration
	}{
		{"0s", -time.Second}, {"12s", 12 * time.Second}, {"41m", 41 * time.Minute}, {"3h5m", 3*time.Hour + 5*time.Minute}, {"2d4h", 52 * time.Hour},
	} {
		assert.Equal(t, c.want, age(c.d))
	}
}

func TestConfirm_Matrix(t *testing.T) {
	ctx := context.Background()
	asked := 0
	ask := func(answer bool) func(context.Context, string) bool {
		return func(_ context.Context, q string) bool {
			asked++
			assert.Equal(t, "Resume? [y/N] ", q)
			return answer
		}
	}
	require.NoError(t, ConfirmResume(ctx, true, ask(false), "p", ""), "--resume goes on without asking")
	assert.Zero(t, asked)
	require.NoError(t, ConfirmResume(ctx, false, ask(true), "p", ""))
	requireCode(t, ConfirmResume(ctx, false, ask(false), "p", ""), exitcode.ExitUsage, "not resumed; p and the files it records are left in place")
	assert.Equal(t, 2, asked)
	err := ConfirmResume(ctx, false, nil, "p", "")
	requireCode(t, err, exitcode.ExitUsage, "a previous run left p; add --resume to continue it (needed when not at a terminal)")
	assert.NotContains(t, err.Error(), "empty directory", "the shared wording suits an import too")

	// The caller says how to start over instead, after both refusals.
	over := "give --checkpoint <another path> to start over"
	requireCode(t, ConfirmResume(ctx, false, nil, "p", over), exitcode.ExitUsage, "(needed when not at a terminal); "+over)
	requireCode(t, ConfirmResume(ctx, false, ask(false), "p", over), exitcode.ExitUsage, "left in place; "+over)

	// Ctrl-C at the question is an interrupt, not a "no".
	canceled, cancel := context.WithCancel(ctx)
	err = ConfirmResume(canceled, false, func(context.Context, string) bool {
		cancel()
		return false
	}, "p", over)
	requireCode(t, err, exitcode.ExitRuntime, "interrupted at the resume question: context canceled; p and the files it records are left in place")
}

func TestPersister_PanicIsAnErrorNotAHang(t *testing.T) {
	var reported []error
	p := NewPersister(context.Background(), &doc{}, func(*doc) error { return nil }, func(err error) { reported = append(reported, err) })
	synced := make(chan error, 1)
	go func() { synced <- p.Sync(func(*doc) { panic("boom") }) }()
	select {
	case err := <-synced:
		require.ErrorContains(t, err, "the progress persister panicked: boom")
	case <-time.After(10 * time.Second):
		t.Fatal("a panic in the persister must not leave Sync waiting")
	}
	require.ErrorContains(t, p.Sync(nil), "panicked", "the persister keeps answering with the error")
	p.Update(func(*doc) {})
	require.ErrorContains(t, p.Close(), "panicked")
	require.Len(t, reported, 1, "onError hears of it once")
}

func TestRemove_AGoneDirectoryIsNotAnError(t *testing.T) {
	require.NoError(t, Remove(filepath.Join(t.TempDir(), "gone", ".p.json")))
}

func TestPersister_UpdateAndSyncAfterCloseAreSafe(t *testing.T) {
	writes := 0
	p := NewPersister(context.Background(), &doc{}, func(*doc) error {
		writes++
		return nil
	}, nil)
	require.NoError(t, p.Sync(nil))
	require.NoError(t, p.Close())
	applied := false
	p.Update(func(*doc) { applied = true })
	require.ErrorIs(t, p.Sync(func(*doc) { applied = true }), ErrClosed)
	assert.False(t, applied, "a change after Close is dropped")
	assert.Equal(t, 1, writes)
	require.NoError(t, p.Close())
}

func TestRetryInUse_TriesAgainWhileTheFileIsInUse(t *testing.T) {
	saved := inUseRetryFor
	inUseRetryFor = time.Second
	t.Cleanup(func() { inUseRetryFor = saved })
	inUse := errors.New("sharing violation")
	retryable := func(err error) bool { return errors.Is(err, inUse) }

	calls := 0
	op := func() error {
		calls++
		if calls < 3 {
			return inUse
		}
		return nil
	}
	require.NoError(t, retryInUse(op, retryable))
	assert.Equal(t, 3, calls, "tried again until the file was free")

	calls = 0
	other := errors.New("no such directory")
	err := retryInUse(func() error { calls++; return other }, retryable)
	require.ErrorIs(t, err, other)
	assert.Equal(t, 1, calls, "any other error is not tried again")

	inUseRetryFor = 20 * time.Millisecond
	calls = 0
	started := time.Now()
	err = retryInUse(func() error { calls++; return inUse }, retryable)
	require.ErrorIs(t, err, inUse, "a file in use for longer than the bound fails")
	assert.Greater(t, calls, 1)
	assert.Less(t, time.Since(started), 500*time.Millisecond)
}

func TestMkdirAll_CreatesEveryLevel(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "a", "b", "c")
	require.NoError(t, MkdirAll(dir, 0o700))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	require.NoError(t, MkdirAll(dir, 0o700), "a directory that is there is fine")
	file := filepath.Join(root, "f")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	assert.Error(t, MkdirAll(file, 0o700), "a file is not a directory")
	assert.Error(t, MkdirAll(filepath.Join(file, "x"), 0o700), "nor below a file")
}

// countingWriter records every write of a persister's state.
type countingWriter struct {
	err    error
	writes []map[string]int
	mu     sync.Mutex
}

func (w *countingWriter) write(s *map[string]int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := map[string]int{}
	for k, v := range *s {
		cp[k] = v
	}
	w.writes = append(w.writes, cp)
	return w.err
}

func (w *countingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

// lazyApplied queues a lazy change and waits until the persister applied it, so that what is
// queued next forms a batch of its own: a lazy change that wrongly writes by itself then shows
// as a write of its own every time.
func lazyApplied(p *Persister[map[string]int], change func(*map[string]int)) {
	applied := make(chan struct{})
	p.UpdateLazy(func(s *map[string]int) {
		change(s)
		close(applied)
	})
	<-applied
}

func TestPersister_LazyChangesRideTheNextWrite(t *testing.T) {
	w := &countingWriter{}
	state := map[string]int{}
	p := NewPersister(context.Background(), &state, w.write, nil)
	lazyApplied(p, func(s *map[string]int) { (*s)["lazy"] = 1 })
	// Sync waits for what was queued before it, the lazy change included.
	require.NoError(t, p.Sync(func(s *map[string]int) { (*s)["synced"] = 1 }))
	require.Equal(t, 1, w.count(), "the lazy change, applied in a batch of its own, wrote nothing")
	assert.Equal(t, map[string]int{"lazy": 1, "synced": 1}, w.writes[0], "the lazy change is written with the next write")

	lazyApplied(p, func(s *map[string]int) { (*s)["lazy"] = 2 })
	require.Equal(t, 1, w.count(), "a lazy change alone writes nothing before Close")
	require.NoError(t, p.Close())
	require.Equal(t, 2, w.count(), "Close writes a pending lazy change exactly once")
	assert.Equal(t, 2, w.writes[1]["lazy"])
	require.NoError(t, p.Close())
	assert.Equal(t, 2, w.count(), "a second Close writes nothing")
}

func TestPersister_LazyAloneWritesNothingUntilClose(t *testing.T) {
	w := &countingWriter{}
	state := map[string]int{}
	p := NewPersister(context.Background(), &state, w.write, nil)
	lazyApplied(p, func(s *map[string]int) { (*s)["lazy"] = 1 })
	lazyApplied(p, func(s *map[string]int) { (*s)["lazy"] = 2 })
	assert.Zero(t, w.count(), "lazy changes alone write nothing")
	require.NoError(t, p.Close())
	require.Equal(t, 1, w.count(), "Close writes them once")
	assert.Equal(t, map[string]int{"lazy": 2}, w.writes[0])

	w1 := &countingWriter{}
	p1 := NewPersister(context.Background(), &state, w1.write, nil)
	lazyApplied(p1, func(s *map[string]int) { (*s)["lazy"] = 3 })
	// A Sync of nothing after the lazy change: one write carries it.
	require.NoError(t, p1.Sync(nil))
	assert.Equal(t, 1, w1.count())
	require.NoError(t, p1.Close())
	assert.Equal(t, 1, w1.count(), "nothing was pending at Close")

	w2 := &countingWriter{}
	p2 := NewPersister(context.Background(), &state, w2.write, nil)
	require.NoError(t, p2.Close())
	assert.Zero(t, w2.count(), "a persister that was given nothing writes nothing")
}

func TestPersister_MinIntervalCoalescesUpdates(t *testing.T) {
	w := &countingWriter{}
	state := map[string]int{}
	p := NewPersisterWith(context.Background(), &state, w.write, nil, PersisterOptions{MinInterval: time.Hour})
	p.Update(func(s *map[string]int) { (*s)["u"] = 1 })
	// The first Update writes at once: no write was made before it.
	lazyApplied(p, func(*map[string]int) {})
	require.Equal(t, 1, w.count())
	for i := 2; i <= 10; i++ {
		p.Update(func(s *map[string]int) { (*s)["u"] = i })
	}
	lazyApplied(p, func(*map[string]int) {})
	assert.Equal(t, 1, w.count(), "Updates within the interval of the last write wait")
	require.NoError(t, p.Sync(func(s *map[string]int) { (*s)["synced"] = 1 }))
	require.Equal(t, 2, w.count(), "a Sync writes at once, with every Update before it")
	assert.Equal(t, map[string]int{"u": 10, "synced": 1}, w.writes[1])
	p.Update(func(s *map[string]int) { (*s)["u"] = 11 })
	require.NoError(t, p.Close())
	require.Equal(t, 3, w.count(), "Close writes the Update still waiting")
	assert.Equal(t, 11, w.writes[2]["u"])
}

// timedWriter records every write of a persister's state with its time.
type timedWriter struct {
	at     []time.Time
	writes []map[string]int
	mu     sync.Mutex
}

func (w *timedWriter) write(s *map[string]int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := map[string]int{}
	for k, v := range *s {
		cp[k] = v
	}
	w.writes = append(w.writes, cp)
	w.at = append(w.at, time.Now())
	return nil
}

// last is the number of writes and the value of "u" in the last one.
func (w *timedWriter) last() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		return 0, 0
	}
	return len(w.writes), w.writes[len(w.writes)-1]["u"]
}

// gaps are the times between consecutive writes from the from-th on.
func (w *timedWriter) gaps(from int) []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []time.Duration
	for i := from + 1; i < len(w.at); i++ {
		out = append(out, w.at[i].Sub(w.at[i-1]))
	}
	return out
}

// The assertions hold however late the goroutines run: an Update writes at once only when the
// interval passed, and the deferred write waits for it, so writes are never closer than it.
func TestPersister_MinIntervalWritesOnceItPassed(t *testing.T) {
	const interval = 200 * time.Millisecond
	w := &timedWriter{}
	state := map[string]int{}
	p := NewPersisterWith(context.Background(), &state, w.write, nil, PersisterOptions{MinInterval: interval})
	defer func() { require.NoError(t, p.Close()) }()
	require.NoError(t, p.Sync(nil))
	for i := 1; i <= 5; i++ {
		p.Update(func(s *map[string]int) { (*s)["u"] = i })
	}
	require.Eventually(t, func() bool { _, u := w.last(); return u == 5 }, 10*time.Second, time.Millisecond,
		"the waiting Updates are written once the interval passed")
	n, _ := w.last()
	assert.LessOrEqual(t, n, 3, "the Sync, and the Updates in at most two writes")
	for _, gap := range w.gaps(0) {
		assert.GreaterOrEqual(t, gap, interval, "writes an Update causes are at least the interval apart")
	}
	time.Sleep(2 * interval)
	after, _ := w.last()
	assert.Equal(t, n, after, "nothing more was waiting")
}

// A Sync writes what an Update deferred: the next deferred write waits the interval from the
// Sync's write, not from the write before it.
func TestPersister_MinIntervalCountsFromASync(t *testing.T) {
	const interval = 300 * time.Millisecond
	w := &timedWriter{}
	state := map[string]int{}
	p := NewPersisterWith(context.Background(), &state, w.write, nil, PersisterOptions{MinInterval: interval})
	defer func() { require.NoError(t, p.Close()) }()
	p.Update(func(s *map[string]int) { (*s)["u"] = 1 })
	require.Eventually(t, func() bool { n, _ := w.last(); return n == 1 }, 10*time.Second, time.Millisecond)
	p.Update(func(s *map[string]int) { (*s)["u"] = 2 })
	time.Sleep(interval / 3)
	require.NoError(t, p.Sync(nil))
	synced, _ := w.last()
	p.Update(func(s *map[string]int) { (*s)["u"] = 3 })
	require.Eventually(t, func() bool { _, u := w.last(); return u == 3 }, 10*time.Second, time.Millisecond)
	for _, gap := range w.gaps(synced - 1) {
		assert.GreaterOrEqual(t, gap, interval, "the Update after the Sync waits the interval from the Sync's write")
	}
}

// Producers as fast as the writes do not grow a batch past the queue.
func TestPersister_BatchIsBoundedByTheQueue(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var written []int
	write := func(s *map[string]int) error {
		once.Do(func() { <-release })
		mu.Lock()
		written = append(written, (*s)["u"])
		mu.Unlock()
		return nil
	}
	state := map[string]int{}
	p := NewPersisterWith(context.Background(), &state, write, nil, PersisterOptions{})
	const updates = 2000
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		for range updates {
			p.Update(func(s *map[string]int) { (*s)["u"]++ })
		}
	}()
	require.Eventually(t, func() bool { return len(p.reqs) == cap(p.reqs) }, 10*time.Second, time.Millisecond, "the first write blocks: the queue fills up")
	close(release)
	<-produced
	require.NoError(t, p.Close())
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, written)
	assert.Equal(t, updates, written[len(written)-1])
	prev := 0
	for _, u := range written {
		assert.LessOrEqual(t, u-prev, cap(p.reqs), "one batch takes at most a queue's worth")
		prev = u
	}
}

func TestPersister_CancelableUpdatesGiveUpOnAFullQueue(t *testing.T) {
	block := make(chan struct{})
	writing := make(chan struct{}, 1)
	write := func(*map[string]int) error {
		select {
		case writing <- struct{}{}:
		default:
		}
		<-block
		return nil
	}
	state := map[string]int{}
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPersisterWith(ctx, &state, write, nil, PersisterOptions{CancelableUpdates: true})
	p.Update(func(*map[string]int) {})
	<-writing // the loop is stuck in the write: the queue fills up
	for i := 0; i < cap(p.reqs); i++ {
		p.Update(func(*map[string]int) {})
	}
	sent := make(chan struct{})
	go func() {
		p.Update(func(*map[string]int) {})
		close(sent)
	}()
	select {
	case <-sent:
		t.Fatal("an Update on a full queue returned before the context ended")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	<-sent
	close(block)
	require.NoError(t, p.Close())
}

func TestIsTemp(t *testing.T) {
	path := filepath.Join("d", "job")
	for name, want := range map[string]bool{
		".job.123.tmp":           true,
		".job.4294967295.tmp":    true,
		".job..tmp":              false,
		".job.tmp":               false,
		".job.12a.tmp":           false,
		".job.v2.123.tmp":        false, // a temporary file of job.v2
		".job-plan.json.123.tmp": false, // a temporary file of the side file job-plan.json
		"job.123.tmp":            false,
		".job.123.tmp.x":         false,
		".jobs.123.tmp":          false,
	} {
		assert.Equal(t, want, IsTemp(path, name), name)
	}
	assert.True(t, IsTemp(SidePath(path, "plan"), ".job-plan.json.123.tmp"))
}

func TestRemoveTemps_LeavesTheTemporaryFilesOfOtherPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job")
	names := []string{".job.1.tmp", ".job.v2.2.tmp", ".job-plan.json.3.tmp", "job", "job.v2"}
	for _, n := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), nil, 0o600))
	}
	require.NoError(t, RemoveTemps(path))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var left []string
	for _, e := range entries {
		left = append(left, e.Name())
	}
	assert.ElementsMatch(t, []string{".job.v2.2.tmp", ".job-plan.json.3.tmp", "job", "job.v2"}, left)
	require.NoError(t, RemoveTemps(filepath.Join(t.TempDir(), "gone", "p")), "a missing directory is no error")
}

// EncodeSide predicts the digest WriteSide returns for the same value, WriteSideBytes writes the
// same file, and ReadSide accepts the side file under it.
func TestEncodeSide_IsWhatWriteSideWrites(t *testing.T) {
	side := SidePath(filepath.Join(t.TempDir(), "p.json"), "plan")
	for _, v := range []any{
		map[string]any{"plan": []string{"a", "b"}},
		struct {
			Items []int `json:"items"`
		}{Items: []int{1, 2, 3}},
		[]string{},
	} {
		data, want, err := EncodeSide(v)
		require.NoError(t, err)
		got, err := WriteSide(side, v)
		require.NoError(t, err)
		assert.Equal(t, got, want)
		written, err := os.ReadFile(side)
		require.NoError(t, err)
		assert.Equal(t, written, data)
		require.NoError(t, os.Remove(side))
		require.NoError(t, WriteSideBytes(side, data))
		var back any
		require.NoError(t, ReadSide(side, want, &back))
	}
	_, _, err := EncodeSide(func() {})
	assert.Error(t, err, "a value JSON cannot encode")
}

// RemoveTemps with side names removes the temporary files of the side files too, in one call.
func TestRemoveTemps_RemovesTheTemporaryFilesOfTheSides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job")
	for _, n := range []string{".job.1.tmp", ".job-plan.json.3.tmp", ".job-cursor.json.4.tmp", ".job.v2.2.tmp"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), nil, 0o600))
	}
	require.NoError(t, RemoveTemps(path, "plan", "cursor"))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, ".job.v2.2.tmp", entries[0].Name())
}

// ReplaceFileUnsynced replaces the file through a temporary file of its own, and RemoveFile
// removes it.
func TestReplaceFileUnsynced_ReplacesTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p-cursor.json")
	require.NoError(t, ReplaceFileUnsynced(path, []byte("a")))
	require.NoError(t, ReplaceFileUnsynced(path, []byte("b")))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "b", string(data))
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	assert.Len(t, entries, 1, "no temporary file is left")
	require.NoError(t, RemoveFile(path))
	assert.ErrorIs(t, RemoveFile(path), os.ErrNotExist)
}

// A side file sits next to its progress file as <file without .json>-<name>.json.
func TestSidePath(t *testing.T) {
	assert.Equal(t, filepath.Join("d", ".export-progress-plan.json"), SidePath(filepath.Join("d", ".export-progress.json"), "plan"))
	assert.Equal(t, "/x/progress-cursor.json", SidePath("/x/progress", "cursor"))
	assert.Equal(t, "p.json-baseline.json", SidePath("p.json.json", "baseline"))
}
