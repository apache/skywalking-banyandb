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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// checkedResume resolves the settings of an import from --input and --checkpoint, then runs
// CheckResume, as data import does before contacting anything.
func checkedResume(t *testing.T, input, checkpoint string, schemaOnly bool) error {
	t.Helper()
	opts := &config.Import{}
	fs := pflag.NewFlagSet("import", pflag.ContinueOnError)
	config.Bind(fs, opts)
	args := []string{"--input", input, "--checkpoint", checkpoint}
	if schemaOnly {
		args = append(args, "--schema-only")
	}
	require.NoError(t, fs.Parse(args))
	sources, err := config.Resolve(fs, func(string) (string, bool) { return "", false }, opts)
	require.NoError(t, err)
	return CheckResume(sources, opts)
}

func requireUsage(t *testing.T, err error, contains, msg string) {
	t.Helper()
	require.Error(t, err, msg)
	assert.Equal(t, exitcode.ExitUsage, exitcode.For(err), msg)
	assert.Contains(t, err.Error(), contains, msg)
}

// A --checkpoint that is a directory, the artifact directory itself among them, is refused up
// front; schema-only uses no progress file and does not check it.
func TestCheckResume_CheckpointIsAFile(t *testing.T) {
	dir := t.TempDir()
	for _, cp := range []string{dir, dir + "/", t.TempDir()} {
		requireUsage(t, checkedResume(t, dir, cp, false), "is a directory; it names the progress file", cp)
		assert.NoError(t, checkedResume(t, dir, cp, true), cp)
	}
	assert.NoError(t, checkedResume(t, dir, dir+"/progress.json", false))
}

// A --checkpoint that is the manifest or a file the manifest lists is refused: --restart would set
// that file aside. Another file under the artifact, the default progress file among them, is not.
func TestCheckResume_CheckpointIsNotAFileOfTheArtifact(t *testing.T) {
	dir := t.TempDir()
	m := &manifest.Manifest{
		Format:      manifest.FormatCSV,
		SchemaFiles: []manifest.File{{Path: "schema/groups.json"}},
		Units: []manifest.Unit{{Sources: []manifest.UnitSource{{
			SegmentFiles: []manifest.File{{Path: "nodes/a/segment.csv"}},
			Shards:       []manifest.Shard{{Files: []manifest.File{{Path: "nodes/a/shard-0.csv.gz"}}}},
		}}}},
	}
	body, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, manifest.FileName), body, 0o600))

	for _, name := range []string{manifest.FileName, "./" + manifest.FileName, "schema/groups.json", "nodes/a/segment.csv", "nodes/a/shard-0.csv.gz"} {
		requireUsage(t, checkedResume(t, dir, filepath.Join(dir, name), false), "is a file of the artifact", name)
	}
	for _, name := range []string{ProgressFileName, "nodes/a/progress.json", "other.json"} {
		assert.NoError(t, checkedResume(t, dir, filepath.Join(dir, name), false), name)
	}
	assert.NoError(t, checkedResume(t, dir, filepath.Join(t.TempDir(), manifest.FileName), false), "a manifest.json of another directory is not this artifact's")
}

// A --checkpoint that ends in the name of a side file would be a side file of another progress
// file, or its own: it is refused, as for an export. A schema-only import has no progress file.
func TestCheckResume_CheckpointIsNotASideFileName(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"x-plan.json", "x-cursor.json", "-plan.json"} {
		err := checkedResume(t, dir, filepath.Join(dir, name), false)
		requireUsage(t, err, "--checkpoint", name)
		assert.Contains(t, err.Error(), "the name of a side file of a progress file")
		assert.NoError(t, checkedResume(t, dir, filepath.Join(dir, name), true), name)
	}
}
