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
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/transfer/progress"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// runResumeState prints, as shell assignments (NAME=value) for eval, the state the resume cases
// (scripts/resume-*.sh) poll:
//
//	resume-state export <export dir>       the progress file, its session, plan, done and inflight
//	                                       units, and the files below nodes/ (.tmp, not listed)
//	resume-state done-files <export dir>   the files the progress file lists for the finished
//	                                       units, one path per line (not an assignment)
//	resume-state files <dir> <list>        a digest of the path, size, modification time and inode
//	                                       of every file the list names, relative to dir
//	resume-state listing <dir>             a digest of every file below dir and its content, but
//	                                       the lock file of the export progress file
//	resume-state plan-units <export dir>   the units the plan side file of the export progress file
//	                                       records, one "<node> <catalog> <group> <stage> <segment>"
//	                                       per line, sorted (not assignments)
//	resume-state manifest-units <dir>      the units of the manifest, one per line like plan-units
//	resume-state import <progress file>    whether the import finished, done items, the rows
//	                                       pushed, started items and the rows they sent after their
//	                                       cursors (the CSV cursor side file merged in per item, as
//	                                       the importer does), in-flight parts, the stream and
//	                                       trace rows pushed, recorded as possible duplicates and
//	                                       pushed by one uninterrupted import, and whether these
//	                                       bound each other per (catalog, group)
//
// The progress files are read the way the exporter and the importer read them. A missing file
// reads as empty; one that exists but cannot be used is an error: they are replaced atomically,
// so it is never half written.
func runResumeState(args []string) int {
	if len(args) < 2 {
		usage()
	}
	var err error
	switch {
	case args[0] == "export" && len(args) == 2:
		err = exportState(args[1])
	case args[0] == "done-files" && len(args) == 2:
		err = doneFiles(args[1])
	case args[0] == "files" && len(args) == 3:
		err = filesState(args[1], args[2])
	case args[0] == "listing" && len(args) == 2:
		err = listingState(args[1])
	case args[0] == "import" && len(args) == 2:
		err = importState(args[1])
	case args[0] == "plan-units" && len(args) == 2:
		err = planUnits(args[1])
	case args[0] == "manifest-units" && len(args) == 2:
		err = manifestUnits(args[1])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "resume-state:", err)
		return 1
	}
	return 0
}

// filesBelow lists every regular file below root, relative to it, sorted; a missing root has none.
func filesBelow(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.Type().IsRegular() {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	slices.Sort(out)
	return out, err
}

// readExport reads the export progress file below dir, an empty one when there is none, and
// reports whether there is one.
func readExport(dir string) (*exporter.ProgressView, bool, error) {
	p, err := progress.ReadExport(filepath.Join(dir, exporter.ProgressFileName))
	if err != nil || p == nil {
		return &exporter.ProgressView{}, false, err
	}
	return p, true, nil
}

func exportState(dir string) error {
	p, hasProgress, err := readExport(dir)
	if err != nil {
		return err
	}
	files, err := filesBelow(dir)
	if err != nil {
		return err
	}
	listed := map[string]bool{}
	for _, d := range p.Done {
		for _, f := range d.Files {
			listed[f] = true
		}
	}
	// Once finished, the manifest lists the files instead of the progress file.
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	hasManifest := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if hasManifest {
		listed = map[string]bool{}
		for _, u := range m.Units {
			for _, src := range u.Sources {
				for _, f := range src.SegmentFiles {
					listed[f.Path] = true
				}
				for _, sh := range src.Shards {
					for _, f := range sh.Files {
						listed[f.Path] = true
					}
				}
			}
		}
	}
	var tmp, unlisted int
	for _, f := range files {
		if !strings.HasPrefix(f, "nodes/") {
			continue
		}
		if strings.HasSuffix(f, ".tmp") {
			tmp++
		}
		if !listed[f] {
			unlisted++
		}
	}
	fmt.Printf("P_PROGRESS=%t\n", hasProgress)
	fmt.Printf("P_SESSION=%s\n", p.SessionID)
	fmt.Printf("P_PLAN=%t\n", len(p.Plan) > 0)
	fmt.Printf("P_DONE=%d\n", len(p.Done))
	fmt.Printf("P_TMP=%d\n", tmp)
	fmt.Printf("P_UNLISTED=%d\n", unlisted)
	fmt.Printf("P_MANIFEST=%t\n", hasManifest)
	return nil
}

func doneFiles(dir string) error {
	p, _, err := readExport(dir)
	if err != nil {
		return err
	}
	for _, d := range p.Done {
		for _, f := range d.Files {
			fmt.Println(f)
		}
	}
	return nil
}

// filesState digests the files list names: a file written again, even with the same bytes, gets
// another modification time or inode.
func filesState(dir, list string) error {
	f, err := os.Open(list)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name := sc.Text()
		info, statErr := os.Stat(filepath.Join(dir, name))
		if errors.Is(statErr, fs.ErrNotExist) {
			fmt.Fprintf(h, "%s\tmissing\n", name)
			continue
		}
		if statErr != nil {
			return statErr
		}
		fmt.Fprintf(h, "%s\t%d\t%d\t%d\n", name, info.Size(), info.ModTime().UnixNano(), inode(info))
	}
	if err = sc.Err(); err != nil {
		return err
	}
	fmt.Printf("P_FILES=%s\n", hex.EncodeToString(h.Sum(nil)))
	return nil
}

func listingState(dir string) error {
	files, err := filesBelow(dir)
	if err != nil {
		return err
	}
	h := sha256.New()
	for _, name := range files {
		if name == filepath.ToSlash(resume.LockPath(exporter.ProgressFileName)) {
			// The lock file of a killed export: the next export locks it and removes it.
			continue
		}
		fmt.Fprintf(h, "%s\t", name)
		if err = hashFile(h, filepath.Join(dir, name)); err != nil {
			return err
		}
		fmt.Fprintln(h)
	}
	fmt.Printf("P_LISTING=%s\n", hex.EncodeToString(h.Sum(nil)))
	return nil
}

func hashFile(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

func importState(path string) error {
	p, err := progress.ReadImport(path)
	if err != nil {
		return err
	}
	hasProgress := p != nil
	if !hasProgress {
		p = &progress.Import{}
	}
	var once uint64
	var violations []string
	if m, mErr := progress.ReadManifest(filepath.Dir(path)); mErr == nil {
		once = p.StreamTraceRowsOnce(m)
		violations = p.StreamTraceBoundViolations(m)
	} else if !errors.Is(mErr, fs.ErrNotExist) {
		return mErr
	}
	for _, v := range violations {
		fmt.Fprintf(os.Stderr, "stream and trace rows of %s\n", v)
	}
	fmt.Printf("P_PROGRESS=%t\n", hasProgress)
	fmt.Printf("P_FINISHED=%t\n", p.FinishedAt != nil)
	fmt.Printf("P_PLAN_ITEMS=%d\n", len(p.Plan))
	// The groups of the planned items, space separated: what the import writes into.
	fmt.Printf("P_GROUPS=%q\n", strings.Join(plannedGroups(p.Plan), " "))
	fmt.Printf("P_COMPLETE=%d\n", p.Complete())
	fmt.Printf("P_ROWS=%d\n", p.PushedRows())
	fmt.Printf("P_STARTED=%d\n", p.Started())
	fmt.Printf("P_SENT_WINDOW=%d\n", p.SentWindow())
	fmt.Printf("P_INFLIGHT=%d\n", len(p.Inflight))
	fmt.Printf("P_INFLIGHT_STREAM_PARTS=%d\n", p.InflightStreamParts())
	fmt.Printf("P_ST_PUSHED=%d\n", p.StreamTracePushed())
	fmt.Printf("P_ST_DUPLICATES=%d\n", p.StreamTraceDuplicates())
	fmt.Printf("P_ST_ONCE=%d\n", once)
	// Per (catalog, group): every row pushed, and no more above one import than the
	// possible duplicates of that group (of a CSV artifact, exactly one import).
	fmt.Printf("P_ST_BOUNDS_OK=%t\n", len(violations) == 0)
	return nil
}

// plannedGroups are the groups of the planned items, sorted: a key is <catalog>/<group>/....
func plannedGroups(plan []importer.PlannedItemView) []string {
	var out []string
	for _, it := range plan {
		if parts := strings.SplitN(it.Key, "/", 3); len(parts) >= 2 && !slices.Contains(out, parts[1]) {
			out = append(out, parts[1])
		}
	}
	slices.Sort(out)
	return out
}

// unitLine names one unit of one node as plan-units and manifest-units print it. A property
// unit has no stage: the manifest keeps one property unit per group over every node, with the
// stage of its first source.
func unitLine(node, catalog, group, stage, segment string) string {
	catalog = strings.ToUpper(catalog)
	if catalog == "PROPERTY" {
		stage = ""
	}
	return strings.Join([]string{node, catalog, group, stage, segment}, " ")
}

// planUnits prints the units of every node the recorded plan of the export in dir lists, the
// way the export writes them into the manifest: one manifest source per planned unit.
func planUnits(dir string) error {
	lines, err := planUnitLines(dir)
	return printLines(lines, err)
}

func planUnitLines(dir string) ([]string, error) {
	p, _, err := readExport(dir)
	if err != nil {
		return nil, err
	}
	if len(p.Plan) == 0 {
		return nil, fmt.Errorf("no plan recorded in %s", dir)
	}
	var lines []string
	for _, f := range p.Plan {
		stage := transfer.NormalizeStage(f.GetStage())
		for _, inv := range f.GetUnits() {
			if p := inv.GetProperty(); p != nil {
				lines = append(lines, unitLine(f.GetNodeId(), transfer.CatalogName(commonv1.Catalog_CATALOG_PROPERTY), p.GetGroup(), "", ""))
				continue
			}
			u := inv.GetSegment().GetUnit()
			lines = append(lines, unitLine(f.GetNodeId(), transfer.CatalogName(u.GetCatalog()), u.GetGroup(), stage, u.GetSegmentSuffix()))
		}
	}
	slices.Sort(lines)
	return lines, nil
}

// manifestUnits prints a line per source of every unit of the manifest in dir.
func manifestUnits(dir string) error {
	lines, err := manifestUnitLines(dir)
	return printLines(lines, err)
}

func manifestUnitLines(dir string) ([]string, error) {
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, u := range m.Units {
		for _, src := range u.Sources {
			lines = append(lines, unitLine(src.Node, u.Catalog, u.Group, u.Stage, u.Segment))
		}
	}
	slices.Sort(lines)
	return lines, nil
}

func printLines(lines []string, err error) error {
	if err != nil {
		return err
	}
	for _, l := range lines {
		fmt.Println(l)
	}
	return nil
}
