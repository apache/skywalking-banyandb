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

// Package migration orchestrates measure / stream data migration: it loads
// the YAML copy plan, classifies each group's catalog through the offline
// schema reader (with a backup-layout fallback), builds the per-group union
// sidx shared by both catalogs, and drives the per-(entry, group) executors
// implemented by banyand/measure and banyand/stream.
package migration

import (
	"fmt"
	"os"
	"regexp"

	"sigs.k8s.io/yaml"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
)

// CopyPlan is the YAML config schema consumed by `migration copy`.
//
// One Source describes WHERE the data + schema-property live — either a
// local backup snapshot (source.backup) or a set of live PVC mount paths
// (source.live). The two modes are mutually exclusive: a plan must specify
// exactly one. Each Entry then names a destination (stage + target) plus
// the subset of node names whose data should be rewritten into that
// entry's target.
type CopyPlan struct {
	// StagingDir is where per-group union sidx artifacts land. Optional;
	// empty means use os.MkdirTemp under TMPDIR. The directory is
	// removed in full when the run finishes (success or failure). If
	// specified, it must not exist yet OR must already be an empty
	// directory — otherwise the loader aborts to avoid clobbering
	// unrelated data.
	StagingDir string `json:"staging_dir,omitempty"`

	Source  CopySource  `json:"source"`
	Groups  []string    `json:"groups"`
	Entries []CopyEntry `json:"entries"`
}

// CopySource is a discriminated union: exactly one of Backup or Live
// must be set.
type CopySource struct {
	Backup *BackupSource `json:"backup,omitempty"`
	Live   *LiveSource   `json:"live,omitempty"`
}

// BackupSource resolves data + schema-property from a backup snapshot
// rooted at Root for the given Date. The on-disk layout under Root
// follows the backup snapshot's standard per-node, per-date directory
// structure.
type BackupSource struct {
	Root string `json:"root"`
	Date string `json:"date"`
}

// LiveSource resolves data + schema-property from live PVC mount paths
// inside the migration runner pod. Each entry's `nodes` list selects the
// (node, root) pairs to migrate for that entry.
type LiveSource struct {
	// Stages maps stage name to the (node, root) pairs that hold that
	// stage's data. Per-stage so the migration can run stage-specific
	// work without ambiguity.
	Stages map[string][]LiveStageNode `json:"stages"`

	// SchemaPropertyPath points the schema loader directly at a
	// schema-property catalog directory exposed by the runner pod.
	SchemaPropertyPath string `json:"schema_property_path"`
}

// LiveStageNode binds a logical node name to a runner-pod path that
// holds that node's data root (the directory whose children are the
// per-group segment trees BanyanDB writes during normal operation).
type LiveStageNode struct {
	Node string `json:"node"`
	Root string `json:"root"`
}

// CopyEntry describes one fan-out destination plus the subset of nodes
// whose source data feeds into it. In backup mode the nodes are backup
// directory names (`hot-0`, `warn-1`, …); in live mode they reference
// LiveSource.Stages[Stage][].Node entries.
type CopyEntry struct {
	Stage  string   `json:"stage"`
	Target string   `json:"target"`
	Nodes  []string `json:"nodes"`
}

var dateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// LoadCopyPlan reads and validates a YAML CopyPlan from path.
func LoadCopyPlan(path string) (*CopyPlan, error) {
	if path == "" {
		return nil, fmt.Errorf("--copy-config is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var plan CopyPlan
	if err := yaml.Unmarshal(raw, &plan); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &plan, nil
}

// Validate enforces the structural rules described on CopyPlan.
//
//nolint:gocyclo // straight-line schema validation; splitting hurts readability.
func (p *CopyPlan) Validate() error {
	if (p.Source.Backup == nil) == (p.Source.Live == nil) {
		return fmt.Errorf("source must set exactly one of backup or live")
	}
	if p.Source.Backup != nil {
		if p.Source.Backup.Root == "" {
			return fmt.Errorf("source.backup.root is required")
		}
		if p.Source.Backup.Date == "" {
			return fmt.Errorf("source.backup.date is required")
		}
		if !dateRE.MatchString(p.Source.Backup.Date) {
			return fmt.Errorf("source.backup.date %q does not match YYYY-MM-DD", p.Source.Backup.Date)
		}
	} else {
		live := p.Source.Live
		if live.SchemaPropertyPath == "" {
			return fmt.Errorf("source.live.schema_property_path is required")
		}
		if len(live.Stages) == 0 {
			return fmt.Errorf("source.live.stages must list at least one stage")
		}
		for stage, pairs := range live.Stages {
			if len(pairs) == 0 {
				return fmt.Errorf("source.live.stages[%q] must list at least one node/root pair", stage)
			}
			seenNode := make(map[string]bool, len(pairs))
			for i, n := range pairs {
				if n.Node == "" {
					return fmt.Errorf("source.live.stages[%q][%d].node is empty", stage, i)
				}
				if n.Root == "" {
					return fmt.Errorf("source.live.stages[%q][%d].root is empty", stage, i)
				}
				if seenNode[n.Node] {
					return fmt.Errorf("source.live.stages[%q] contains duplicate node %q", stage, n.Node)
				}
				seenNode[n.Node] = true
			}
		}
	}
	if len(p.Groups) == 0 {
		return fmt.Errorf("groups must list at least one group")
	}
	seenGroup := make(map[string]bool, len(p.Groups))
	for _, g := range p.Groups {
		if g == "" {
			return fmt.Errorf("groups contains an empty entry")
		}
		if seenGroup[g] {
			return fmt.Errorf("groups contains duplicate entry %q", g)
		}
		seenGroup[g] = true
	}
	if len(p.Entries) == 0 {
		return fmt.Errorf("entries must list at least one (stage, target) destination")
	}
	seenTarget := make(map[string]int, len(p.Entries))
	for i, e := range p.Entries {
		if e.Stage == "" {
			return fmt.Errorf("entries[%d].stage is required", i)
		}
		if e.Target == "" {
			return fmt.Errorf("entries[%d].target is required", i)
		}
		if len(e.Nodes) == 0 {
			return fmt.Errorf("entries[%d].nodes must list at least one node", i)
		}
		seenNode := make(map[string]bool, len(e.Nodes))
		for j, n := range e.Nodes {
			if n == "" {
				return fmt.Errorf("entries[%d].nodes[%d] is empty", i, j)
			}
			if seenNode[n] {
				return fmt.Errorf("entries[%d].nodes contains duplicate %q", i, n)
			}
			seenNode[n] = true
		}
		if p.Source.Live != nil {
			if _, stageExists := p.Source.Live.Stages[e.Stage]; !stageExists {
				return fmt.Errorf("entries[%d].stage %q is not defined in source.live.stages", i, e.Stage)
			}
			// every entry node must match a Stages[stage][].Node
			validNodes := map[string]bool{}
			for _, n := range p.Source.Live.Stages[e.Stage] {
				validNodes[n.Node] = true
			}
			for _, n := range e.Nodes {
				if !validNodes[n] {
					return fmt.Errorf("entries[%d].nodes contains %q which is not defined under source.live.stages[%q]",
						i, n, e.Stage)
				}
			}
		}
		if prev, dup := seenTarget[e.Target]; dup {
			return fmt.Errorf("entries[%d].target %q duplicates entries[%d].target",
				i, e.Target, prev)
		}
		seenTarget[e.Target] = i
	}
	return nil
}

// PrepareStagingDir resolves the plan's staging dir: it returns either
// the user-specified path (after asserting it is absent or empty) or a
// fresh os.MkdirTemp directory. Callers must defer os.RemoveAll on the
// returned path to honor the "cleanup after run" contract.
func (p *CopyPlan) PrepareStagingDir() (string, error) {
	configured := p.StagingDir
	if configured == "" {
		return os.MkdirTemp("", "banyandb-migration-staging-*")
	}
	info, err := os.Stat(configured)
	if err == nil {
		if !info.IsDir() {
			return "", fmt.Errorf("staging_dir %q exists and is not a directory", configured)
		}
		entries, readErr := os.ReadDir(configured)
		if readErr != nil {
			return "", fmt.Errorf("inspect staging_dir %q: %w", configured, readErr)
		}
		if len(entries) > 0 {
			return "", fmt.Errorf("staging_dir %q is not empty — refusing to use it (delete it first to confirm)", configured)
		}
		return configured, nil
	}
	if !os.IsNotExist(err) {
		return "", fmt.Errorf("stat staging_dir %q: %w", configured, err)
	}
	if mkErr := os.MkdirAll(configured, storage.DirPerm); mkErr != nil {
		return "", fmt.Errorf("create staging_dir %q: %w", configured, mkErr)
	}
	return configured, nil
}

// ResolvedEntry is a CopyEntry with its source roots resolved for the
// plan's mode: in live mode Source carries each node's data root in Nodes
// order; in backup mode Source stays empty and the per-group roots derive
// from BackupDir + Date + Nodes at lookup time.
type ResolvedEntry struct {
	Stage  string
	Target string
	Source []string
	Nodes  []string
}

// ResolvedEntries lifts the per-mode source resolution that the former
// per-catalog config converters performed, once for the whole plan.
func (p *CopyPlan) ResolvedEntries() []ResolvedEntry {
	entries := make([]ResolvedEntry, len(p.Entries))
	if p.Source.Backup != nil {
		for i, e := range p.Entries {
			entries[i] = ResolvedEntry{
				Stage:  e.Stage,
				Target: e.Target,
				Nodes:  append([]string(nil), e.Nodes...),
			}
		}
		return entries
	}
	for i, e := range p.Entries {
		rootByNode := map[string]string{}
		for _, n := range p.Source.Live.Stages[e.Stage] {
			rootByNode[n.Node] = n.Root
		}
		srcs := make([]string, 0, len(e.Nodes))
		for _, n := range e.Nodes {
			srcs = append(srcs, rootByNode[n])
		}
		// Nodes is also propagated in live mode so log lines that
		// reference the entry by its node identifier (hot-0, …)
		// stay informative — Source carries the same info as paths
		// but is noisier in operator-facing output.
		entries[i] = ResolvedEntry{
			Stage:  e.Stage,
			Target: e.Target,
			Source: srcs,
			Nodes:  append([]string(nil), e.Nodes...),
		}
	}
	return entries
}
