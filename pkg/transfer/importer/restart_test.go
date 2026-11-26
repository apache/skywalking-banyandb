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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// A dry run with --restart reads no progress file: it previews the plan of a fresh import, whose
// progress file the real run sets aside, whatever import the file records.
func TestDryRunRestart_PreviewsAFreshImport(t *testing.T) {
	for _, tc := range []struct {
		edit func(*progressState)
		name string
	}{
		{name: "interrupted"},
		{name: "foreign", edit: func(s *progressState) {
			s.Bindings.Liaisons, s.Bindings.TargetNodes = []string{"elsewhere"}, []string{"other"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLiaison()
			li := f.serve(t)
			opts := resumeImport(t, writeDataArtifact(t))
			interruptedCSV(t, f, li, opts, 5)
			path := ProgressPath(opts)
			if tc.edit != nil {
				editState(t, path, tc.edit)
			}
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			batches := len(f.receivedBatches())

			opts.DryRun, opts.Restart = true, true
			dry := runImportEnv(context.Background(), f, li, opts, nil, nil)
			require.NoError(t, dry.err, dry.log)
			assert.Contains(t, dry.log, "NOTE --restart: the plan below is that of a fresh import, "+path+" is not read; the real run keeps it as "+
				path+".<timestamp>.bak")
			assert.NotContains(t, dry.log, "RESUME   ", "no resume summary")
			assert.Nil(t, dry.res.Plan.Resumed)
			assert.Len(t, dry.res.Plan.Entries, 3, "the full plan")
			assert.Len(t, f.receivedBatches(), batches)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, before, after, "the progress file is left as it is")

			if tc.edit != nil {
				opts.Restart = false
				refused := runImportEnv(context.Background(), f, li, opts, nil, nil)
				assert.Equal(t, exitcode.ExitResume, refused.code(), "without --restart the dry run reads the foreign progress file: %s", refused.log)
			}
		})
	}
}

// The --restart warning says what the earlier import really stored: nothing twice for another
// import's progress file or one that pushed nothing; the groups it pushed otherwise.
func TestRestart_WarningDescribesTheEarlierImport(t *testing.T) {
	for _, tc := range []struct {
		prepare func(t *testing.T, f *fakeLiaison, path string, opts func() importRun)
		name    string
		want    string
		not     string
	}{
		{
			name: "finished",
			prepare: func(t *testing.T, _ *fakeLiaison, _ string, run func() importRun) {
				first := run()
				require.NoError(t, first.err, first.log)
			},
			want: "the stream and trace rows an earlier run already imported are stored twice, in stream sw_stream",
		},
		{
			name: "foreign",
			prepare: func(t *testing.T, _ *fakeLiaison, path string, run func() importRun) {
				first := run()
				require.NoError(t, first.err, first.log)
				editState(t, path, func(s *progressState) { s.Bindings.ManifestDigest = "sha256:another" })
			},
			want: "it records the import of another artifact, target or options, so nothing it records is stored twice by this one " +
				"(it differs in: manifestDigest",
			not: "are stored twice",
		},
		{
			name: "nothing pushed",
			prepare: func(t *testing.T, _ *fakeLiaison, path string, run func() importRun) {
				first := run()
				require.NoError(t, first.err, first.log)
				editState(t, path, func(s *progressState) {
					s.Done, s.Inflight, s.Pushed, s.FinishedAt = map[string]*itemRecord{}, map[string]inflightPayload{}, []rowCount{}, nil
				})
			},
			want: "it records nothing pushed yet, so nothing is stored twice",
			not:  "are stored twice",
		},
		{
			name: "measures only",
			prepare: func(t *testing.T, _ *fakeLiaison, path string, run func() importRun) {
				first := run()
				require.NoError(t, first.err, first.log)
				editState(t, path, func(s *progressState) {
					for k := range s.Done {
						if groupOfKey(k) != "measure sw_metric" {
							delete(s.Done, k)
						}
					}
					s.Pushed = []rowCount{{Catalog: "measure", Group: "sw_metric", Stage: "hot", Kind: "shard", Rows: 2}}
				})
			},
			want: "nothing of this plan is stored twice: measures and properties overwrite what is there",
		},
		{
			name: "unreadable",
			prepare: func(t *testing.T, _ *fakeLiaison, path string, _ func() importRun) {
				require.NoError(t, os.WriteFile(path, []byte("not json"), 0o600))
			},
			want: "so what it records is unknown: if an earlier run imported this artifact into this cluster, " +
				"the stream and trace rows an earlier run already imported may be stored twice, in stream sw_stream",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeLiaison()
			li := f.serve(t)
			opts := resumeImport(t, writeDataArtifact(t))
			path := ProgressPath(opts)
			tc.prepare(t, f, path, func() importRun { return runImportEnv(context.Background(), f, li, opts, nil, nil) })
			opts.Restart = true
			restarted := runImportEnv(context.Background(), f, li, opts, nil, nil)
			require.NoError(t, restarted.err, restarted.log)
			assert.Contains(t, restarted.log, "WARN --restart: "+path+" is kept as "+path+".")
			assert.Contains(t, restarted.log, tc.want)
			if tc.not != "" {
				assert.NotContains(t, restarted.log, tc.not)
			}
		})
	}
}

// --restart imports from scratch: every row is checked again, the items an earlier run completed
// too.
func TestResumeCSV_RestartChecksEveryRow(t *testing.T) {
	fastPolls(t)
	f := newFakeLiaison()
	li := f.serve(t)
	opts := resumeImport(t, writeDataArtifact(t))
	interruptedCSV(t, f, li, opts, 5)
	opts.Restart = true
	run := runImportEnv(context.Background(), f, li, opts, nil, nil)
	require.NoError(t, run.err, run.log)
	require.NotNil(t, run.res.Plan.RowCheck)
	assert.Equal(t, 3, run.res.Plan.RowCheck.Items)
}
