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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validBackupPlan returns a minimal CopyPlan in backup mode that
// passes Validate. Tests mutate fields to assert per-rule failures.
func validBackupPlan() *CopyPlan {
	return &CopyPlan{
		Source: CopySource{
			Backup: &BackupSource{
				Root: "/tmp/backup",
				Date: "2026-05-19",
			},
		},
		Groups: []string{"sw_metricsMinute"},
		Entries: []CopyEntry{
			{Stage: "hot", Target: "/tmp/hot", Nodes: []string{"hot-0"}},
		},
	}
}

// validLivePlan returns a minimal CopyPlan in live mode.
func validLivePlan() *CopyPlan {
	return &CopyPlan{
		Source: CopySource{
			Live: &LiveSource{
				SchemaPropertyPath: "/mnt/hot-0/schema-property/data/_schema",
				Stages: map[string][]LiveStageNode{
					"hot": {{Node: "hot-0", Root: "/mnt/hot-0/measure/data"}},
				},
			},
		},
		Groups: []string{"sw_metricsMinute"},
		Entries: []CopyEntry{
			{Stage: "hot", Target: "/mnt/hot-0/measure/data-copy", Nodes: []string{"hot-0"}},
		},
	}
}

func TestValidate_okBackup(t *testing.T) {
	if err := validBackupPlan().Validate(); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestValidate_okLive(t *testing.T) {
	if err := validLivePlan().Validate(); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
}

func TestValidate_sourceXOR_neither(t *testing.T) {
	p := validBackupPlan()
	p.Source = CopySource{}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one of backup or live") {
		t.Fatalf("expected xor error, got %v", err)
	}
}

func TestValidate_sourceXOR_both(t *testing.T) {
	p := validBackupPlan()
	p.Source.Live = &LiveSource{
		SchemaPropertyPath: "/p",
		Stages: map[string][]LiveStageNode{
			"hot": {{Node: "hot-0", Root: "/r"}},
		},
	}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one of backup or live") {
		t.Fatalf("expected xor error, got %v", err)
	}
}

func TestValidate_backupMissingRoot(t *testing.T) {
	p := validBackupPlan()
	p.Source.Backup.Root = ""
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "source.backup.root") {
		t.Fatalf("expected root error, got %v", err)
	}
}

func TestValidate_backupBadDate(t *testing.T) {
	p := validBackupPlan()
	p.Source.Backup.Date = "2026/05/19"
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "YYYY-MM-DD") {
		t.Fatalf("expected date format error, got %v", err)
	}
}

func TestValidate_liveMissingSchemaPropertyPath(t *testing.T) {
	p := validLivePlan()
	p.Source.Live.SchemaPropertyPath = ""
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "schema_property_path") {
		t.Fatalf("expected schema_property_path error, got %v", err)
	}
}

func TestValidate_liveEmptyStages(t *testing.T) {
	p := validLivePlan()
	p.Source.Live.Stages = nil
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "stages") {
		t.Fatalf("expected stages error, got %v", err)
	}
}

func TestValidate_liveStagePairsEmpty(t *testing.T) {
	p := validLivePlan()
	p.Source.Live.Stages["hot"] = nil
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "node/root pair") {
		t.Fatalf("expected pairs error, got %v", err)
	}
}

func TestValidate_liveDuplicateNodeInStage(t *testing.T) {
	p := validLivePlan()
	p.Source.Live.Stages["hot"] = []LiveStageNode{
		{Node: "hot-0", Root: "/a"},
		{Node: "hot-0", Root: "/b"},
	}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestValidate_emptyGroups(t *testing.T) {
	p := validBackupPlan()
	p.Groups = nil
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "groups") {
		t.Fatalf("expected groups error, got %v", err)
	}
}

func TestValidate_duplicateGroup(t *testing.T) {
	p := validBackupPlan()
	p.Groups = []string{"a", "a"}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate group error, got %v", err)
	}
}

func TestValidate_emptyEntries(t *testing.T) {
	p := validBackupPlan()
	p.Entries = nil
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "entries") {
		t.Fatalf("expected entries error, got %v", err)
	}
}

func TestValidate_entryMissingStageOrTarget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		need  string
		entry CopyEntry
	}{
		{"stage", "stage", CopyEntry{Stage: "", Target: "/t", Nodes: []string{"hot-0"}}},
		{"target", "target", CopyEntry{Stage: "hot", Target: "", Nodes: []string{"hot-0"}}},
		{"nodes", "nodes", CopyEntry{Stage: "hot", Target: "/t", Nodes: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := validBackupPlan()
			p.Entries = []CopyEntry{tc.entry}
			if err := p.Validate(); err == nil || !strings.Contains(err.Error(), tc.need) {
				t.Fatalf("expected %s error, got %v", tc.need, err)
			}
		})
	}
}

func TestValidate_duplicateTarget(t *testing.T) {
	p := validBackupPlan()
	p.Entries = []CopyEntry{
		{Stage: "hot", Target: "/same", Nodes: []string{"hot-0"}},
		{Stage: "warm", Target: "/same", Nodes: []string{"warn-0"}},
	}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("expected duplicate target error, got %v", err)
	}
}

func TestValidate_liveEntryNodeNotDefined(t *testing.T) {
	p := validLivePlan()
	p.Entries[0].Nodes = []string{"unknown-node"}
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "not defined under source.live.stages") {
		t.Fatalf("expected undefined-node error, got %v", err)
	}
}

func TestLoadCopyPlan_roundtripLive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.yaml")
	yamlContent := `
source:
  live:
    schema_property_path: /mnt/hot-0/schema-property/data/_schema
    stages:
      hot:
        - { node: hot-0, root: /mnt/hot-0/measure/data }
        - { node: hot-1, root: /mnt/hot-1/measure/data }
groups:
  - sw_metricsMinute
entries:
  - stage: hot
    target: /mnt/hot-0/measure/data-copy
    nodes: [hot-0]
`
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	plan, err := LoadCopyPlan(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if plan.Source.Live == nil {
		t.Fatalf("expected live source, got %+v", plan.Source)
	}
	if got := plan.Source.Live.Stages["hot"]; len(got) != 2 || got[0].Node != "hot-0" {
		t.Fatalf("unexpected stages: %+v", got)
	}
	if plan.Entries[0].Nodes[0] != "hot-0" {
		t.Fatalf("unexpected entry node: %+v", plan.Entries[0])
	}
}

func TestPrepareStagingDir_emptyConfiguredUsesTmpDir(t *testing.T) {
	dir, err := PrepareStagingDir("")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer os.RemoveAll(dir)
	if !strings.Contains(dir, "banyandb-migration-staging-") {
		t.Fatalf("expected MkdirTemp prefix, got %s", dir)
	}
}

func TestPrepareStagingDir_emptyExistingDirIsReused(t *testing.T) {
	base := t.TempDir()
	got, err := PrepareStagingDir(base)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got != base {
		t.Fatalf("expected %s, got %s", base, got)
	}
}

func TestPrepareStagingDir_existingNonEmptyRejected(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "marker"), nil, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := PrepareStagingDir(base); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("expected non-empty rejection, got %v", err)
	}
}

func TestPrepareStagingDir_createsNonExistingDir(t *testing.T) {
	base := filepath.Join(t.TempDir(), "subdir-that-does-not-exist")
	got, err := PrepareStagingDir(base)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got != base {
		t.Fatalf("expected %s, got %s", base, got)
	}
	info, statErr := os.Stat(base)
	if statErr != nil || !info.IsDir() {
		t.Fatalf("expected dir created, stat=%v info=%+v", statErr, info)
	}
}
