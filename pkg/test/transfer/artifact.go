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

package transfer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// CSVFile is one decoded CSV file of an artifact.
type CSVFile struct {
	// Path is the artifact-relative slash path: nodes/<dir>/<catalog>/<group>/...
	Path    string
	NodeDir string
	Catalog string
	Group   string
	// Segment is the segment suffix; "" for property files.
	Segment string
	// Prefix is the path without the "-<seq>.csv[.gz]" tail, shared by the files one writer rolled.
	Prefix   string
	Header   []string
	Datatype []string
	Rows     [][]string
	Shard    int
	Seq      int
	// SegmentLevel marks the segment-<seq> file of index-mode measure rows.
	SegmentLevel bool
}

// Row returns the row i keyed by header.
func (f *CSVFile) Row(i int) map[string]string {
	out := make(map[string]string, len(f.Header))
	for j, h := range f.Header {
		out[h] = f.Rows[i][j]
	}
	return out
}

// Artifact is a decoded export output directory.
type Artifact struct {
	Manifest *manifest.Manifest
	Dir      string
	Files    []*CSVFile
}

var (
	shardFileRe   = regexp.MustCompile(`^shard-(\d+)-(\d{5})\.csv(\.gz)?$`)
	segmentFileRe = regexp.MustCompile(`^segment-(\d{5})\.csv(\.gz)?$`)
)

// ReadArtifact reads manifest.json and decodes every CSV file below nodes/. annotations
// tells whether every file must start with the #datatype line before its header.
func ReadArtifact(dir string, annotations bool) *Artifact {
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	a := &Artifact{Dir: dir, Manifest: m}
	nodes := filepath.Join(dir, "nodes")
	if _, statErr := os.Stat(nodes); os.IsNotExist(statErr) {
		return a
	}
	err = filepath.WalkDir(nodes, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, relErr := filepath.Rel(dir, p)
		if relErr != nil {
			return relErr
		}
		f, parseErr := decodeCSVFile(dir, filepath.ToSlash(rel), annotations)
		if parseErr != nil {
			return parseErr
		}
		a.Files = append(a.Files, f)
		return nil
	})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	sort.Slice(a.Files, func(i, j int) bool { return a.Files[i].Path < a.Files[j].Path })
	return a
}

func decodeCSVFile(root, rel string, annotations bool) (*CSVFile, error) {
	parts := strings.Split(rel, "/")
	// nodes/<dir>/<catalog>/<group>/[seg-<suffix>/]<leaf>
	if len(parts) < 5 || parts[0] != "nodes" {
		return nil, fmt.Errorf("unexpected artifact path %s", rel)
	}
	f := &CSVFile{Path: rel, NodeDir: parts[1], Catalog: parts[2], Group: parts[3], Shard: -1}
	leaf := parts[len(parts)-1]
	switch {
	case f.Catalog == "property" && len(parts) == 5:
	case f.Catalog != "property" && len(parts) == 6 && strings.HasPrefix(parts[4], "seg-"):
		f.Segment = strings.TrimPrefix(parts[4], "seg-")
	default:
		return nil, fmt.Errorf("unexpected artifact path %s", rel)
	}
	gz := strings.HasSuffix(leaf, ".gz")
	if mm := shardFileRe.FindStringSubmatch(leaf); mm != nil {
		f.Shard, _ = strconv.Atoi(mm[1])
		f.Seq, _ = strconv.Atoi(mm[2])
		f.Prefix = path.Join(path.Dir(rel), "shard-"+mm[1])
	} else if mm = segmentFileRe.FindStringSubmatch(leaf); mm != nil && f.Catalog == "measure" {
		f.SegmentLevel = true
		f.Seq, _ = strconv.Atoi(mm[1])
		f.Prefix = path.Join(path.Dir(rel), "segment")
	} else {
		return nil, fmt.Errorf("unexpected file name %s", rel)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		return nil, err
	}
	var r io.Reader = bytes.NewReader(raw)
	if gz {
		zr, zErr := gzip.NewReader(r)
		if zErr != nil {
			return nil, fmt.Errorf("%s: %w", rel, zErr)
		}
		defer zr.Close()
		r = zr
	}
	cr := csv.NewReader(bufio.NewReader(r))
	cr.FieldsPerRecord = -1
	records, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", rel, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s has no header", rel)
	}
	if annotations {
		// The #datatype line precedes the header and has one more cell.
		if len(records[0]) == 0 || records[0][0] != "#datatype" {
			return nil, fmt.Errorf("%s does not start with a #datatype line", rel)
		}
		f.Datatype, records = records[0], records[1:]
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s has no header", rel)
	}
	f.Header, records = records[0], records[1:]
	if f.Datatype != nil && len(f.Datatype) != len(f.Header)+1 {
		return nil, fmt.Errorf("%s: #datatype has %d cells for %d columns", rel, len(f.Datatype), len(f.Header))
	}
	for i, rec := range records {
		if len(rec) != len(f.Header) {
			return nil, fmt.Errorf("%s row %d has %d cells for %d columns", rel, i, len(rec), len(f.Header))
		}
	}
	f.Rows = records
	return f, nil
}

// FilesOf returns the files of one catalog and group, across nodes.
func (a *Artifact) FilesOf(catalog, group string) []*CSVFile {
	var out []*CSVFile
	for _, f := range a.Files {
		if f.Catalog == catalog && f.Group == group {
			out = append(out, f)
		}
	}
	return out
}

// RowsOf returns the rows of one resource in one catalog and group, per node directory,
// keyed by header.
func (a *Artifact) RowsOf(catalog, group, name string) map[string][]map[string]string {
	out := map[string][]map[string]string{}
	for _, f := range a.FilesOf(catalog, group) {
		for i := range f.Rows {
			if f.Rows[i][0] == name {
				out[f.NodeDir] = append(out[f.NodeDir], f.Row(i))
			}
		}
	}
	return out
}

// LatestProperties returns the latest revision of every entity of the suites' property
// resource across nodes, keyed by _id: the row with the largest _mod_revision, a tombstone
// winning over a live row of the same revision. Older revisions are dropped.
func (a *Artifact) LatestProperties() map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, rows := range a.RowsOf("property", PropertyGroup, PropertyName) {
		for _, r := range rows {
			cur, ok := out[r["_id"]]
			if !ok {
				out[r["_id"]] = r
				continue
			}
			rev, err := strconv.ParseInt(r["_mod_revision"], 10, 64)
			gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "_mod_revision of %s", r["_id"])
			curRev, err := strconv.ParseInt(cur["_mod_revision"], 10, 64)
			gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "_mod_revision of %s", r["_id"])
			if rev > curRev || (rev == curRev && !IsLiveProperty(r) && IsLiveProperty(cur)) {
				out[r["_id"]] = r
			}
		}
	}
	return out
}

// IsLiveProperty reports whether a property row is live: its _delete_time is 0 or null.
func IsLiveProperty(r map[string]string) bool {
	return r["_delete_time"] == "0" || IsNull(r["_delete_time"])
}

// LiveProperty returns the latest revision of the first entity, by _id, of the suites'
// property resource whose latest revision is live and that is not the tombstone, or nil.
func (a *Artifact) LiveProperty() map[string]string {
	latest := a.LatestProperties()
	for _, id := range slices.Sorted(maps.Keys(latest)) {
		if r := latest[id]; IsLiveProperty(r) && id != TombstoneProperty().GetId() {
			return r
		}
	}
	return nil
}

// VerifyManifest checks the manifest against the files on disk: the file sets are equal,
// every file has the announced bytes and CRC-32, the rowCounts of every shard and of every
// source's segment files are the rows its files hold and totalCount is their sum, the schema
// files exist and every source node has its directory.
func (a *Artifact) VerifyManifest() {
	m := a.Manifest
	gomega.ExpectWithOffset(1, m.Format).To(gomega.Equal("csv"))
	gomega.ExpectWithOffset(1, m.ArtifactVersion).To(gomega.Equal(manifest.ArtifactVersion))
	checkFile := func(f manifest.File) {
		raw, err := os.ReadFile(filepath.Join(a.Dir, filepath.FromSlash(f.Path)))
		gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred(), f.Path)
		gomega.ExpectWithOffset(2, uint64(len(raw))).To(gomega.Equal(f.Bytes), "bytes of %s", f.Path)
		gomega.ExpectWithOffset(2, manifest.FormatCRC32(crc32.ChecksumIEEE(raw))).To(gomega.Equal(f.CRC32), "crc32 of %s", f.Path)
	}
	gomega.ExpectWithOffset(1, m.SchemaFiles).NotTo(gomega.BeEmpty())
	for _, f := range m.SchemaFiles {
		checkFile(f)
	}
	nodeDirs := map[string]string{}
	for _, n := range m.Source.Nodes {
		gomega.ExpectWithOffset(1, n.Dir).To(gomega.Equal(manifest.NodeDir(n.ID)))
		nodeDirs[n.ID] = n.Dir
	}
	held := map[string]map[string]uint64{}
	for _, f := range a.Files {
		got := map[string]uint64{}
		for _, r := range f.Rows {
			got[r[0]]++
		}
		held[f.Path] = got
	}
	// checkCounts expects rowCounts to be the rows files hold and totalCount their sum.
	checkCounts := func(files []manifest.File, rowCounts map[string]uint64, total uint64, what string) {
		want := map[string]uint64{}
		var sum uint64
		for _, f := range files {
			for name, n := range held[f.Path] {
				want[name] += n
				sum += n
			}
		}
		got := rowCounts
		if got == nil {
			got = map[string]uint64{}
		}
		gomega.ExpectWithOffset(2, got).To(gomega.Equal(want), "rowCounts of %s", what)
		gomega.ExpectWithOffset(2, total).To(gomega.Equal(sum), "totalCount of %s", what)
	}
	listed := map[string]bool{}
	for _, u := range m.Units {
		for _, src := range u.Sources {
			dir, ok := nodeDirs[src.Node]
			gomega.ExpectWithOffset(1, ok).To(gomega.BeTrue(), "unit source %s is a manifest node", src.Node)
			prefix := "nodes/" + dir + "/"
			for _, f := range src.SegmentFiles {
				gomega.ExpectWithOffset(1, f.Path).To(gomega.HavePrefix(prefix))
				checkFile(f)
				listed[f.Path] = true
			}
			checkCounts(src.SegmentFiles, src.RowCounts, src.TotalCount,
				fmt.Sprintf("the segment files of %s/%s/%s on %s", u.Catalog, u.Group, u.Segment, src.Node))
			for _, sh := range src.Shards {
				for _, f := range sh.Files {
					gomega.ExpectWithOffset(1, f.Path).To(gomega.HavePrefix(prefix))
					checkFile(f)
					listed[f.Path] = true
				}
				checkCounts(sh.Files, sh.RowCounts, sh.TotalCount,
					fmt.Sprintf("%s/%s/%s shard %d on %s", u.Catalog, u.Group, u.Segment, sh.Shard, src.Node))
			}
		}
	}
	onDisk := map[string]bool{}
	for _, f := range a.Files {
		onDisk[f.Path] = true
	}
	gomega.ExpectWithOffset(1, onDisk).To(gomega.Equal(listed), "the manifest lists exactly the files below nodes/")
}

// NodeDirs returns the directories below nodes/ that hold files.
func (a *Artifact) NodeDirs() []string {
	seen := map[string]bool{}
	for _, f := range a.Files {
		seen[f.NodeDir] = true
	}
	out := make([]string, 0, len(seen))
	for d := range seen {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// Schemas decodes one schema/<kind>.jsonl file into messages created by newMsg.
func Schemas[T proto.Message](dir, kind string, newMsg func() T) []T {
	raw, err := os.ReadFile(filepath.Join(dir, "schema", kind+".jsonl"))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	var out []T
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		msg := newMsg()
		gomega.ExpectWithOffset(1, protojson.Unmarshal([]byte(line), msg)).To(gomega.Succeed())
		out = append(out, msg)
	}
	return out
}

// StreamsOf returns the streams of a group from the artifact's schema, sorted by name.
func StreamsOf(dir, group string) []*databasev1.Stream {
	var out []*databasev1.Stream
	for _, s := range Schemas(dir, "stream", func() *databasev1.Stream { return &databasev1.Stream{} }) {
		if s.GetMetadata().GetGroup() == group {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetMetadata().GetName() < out[j].GetMetadata().GetName() })
	return out
}

// MeasuresOf returns the measures of a group from the artifact's schema, sorted by name.
func MeasuresOf(dir, group string) []*databasev1.Measure {
	var out []*databasev1.Measure
	for _, s := range Schemas(dir, "measure", func() *databasev1.Measure { return &databasev1.Measure{} }) {
		if s.GetMetadata().GetGroup() == group {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetMetadata().GetName() < out[j].GetMetadata().GetName() })
	return out
}

// TracesOf returns the traces of a group from the artifact's schema, sorted by name.
func TracesOf(dir, group string) []*databasev1.Trace {
	var out []*databasev1.Trace
	for _, s := range Schemas(dir, "trace", func() *databasev1.Trace { return &databasev1.Trace{} }) {
		if s.GetMetadata().GetGroup() == group {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetMetadata().GetName() < out[j].GetMetadata().GetName() })
	return out
}

// PropertiesOf returns the property schemas of a group from the artifact's schema, sorted by name.
func PropertiesOf(dir, group string) []*databasev1.Property {
	var out []*databasev1.Property
	for _, s := range Schemas(dir, "property", func() *databasev1.Property { return &databasev1.Property{} }) {
		if s.GetMetadata().GetGroup() == group {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetMetadata().GetName() < out[j].GetMetadata().GetName() })
	return out
}

// ExpectAbortedOutput asserts what an aborted export leaves behind: no manifest, and every
// file renamed into place below nodes/ a complete, decodable CSV file. A file cut short keeps
// its .tmp name.
func ExpectAbortedOutput(dir string) {
	gomega.ExpectWithOffset(1, filepath.Join(dir, manifest.FileName)).NotTo(gomega.BeAnExistingFile())
	nodes := filepath.Join(dir, "nodes")
	if _, err := os.Stat(nodes); os.IsNotExist(err) {
		return
	}
	err := filepath.WalkDir(nodes, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || strings.HasSuffix(p, ".tmp") {
			return walkErr
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		_, err = decodeCSVFile(dir, filepath.ToSlash(rel), false)
		return err
	})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
}

// ExpectReplicasEqual asserts that every shard listed by more than one source of a unit
// holds the same rows under each source, and returns how many such shards it compared. The
// delete time of a property tombstone is compared only for being set.
func (a *Artifact) ExpectReplicasEqual() int {
	byPath := map[string]*CSVFile{}
	for _, f := range a.Files {
		byPath[f.Path] = f
	}
	var multi int
	for _, u := range a.Manifest.Units {
		rows := map[uint32]map[string][]string{}
		for _, src := range u.Sources {
			for _, sh := range src.Shards {
				if rows[sh.Shard] == nil {
					rows[sh.Shard] = map[string][]string{}
				}
				var got []string
				for _, f := range sh.Files {
					cf := byPath[f.Path]
					for _, r := range cf.Rows {
						r = append([]string(nil), r...)
						for i, h := range cf.Header {
							// Every replica stamps a property deletion with its own clock.
							if h == "_delete_time" && r[i] != "0" {
								r[i] = "<deleted>"
							}
						}
						got = append(got, strings.Join(r, "\x00"))
					}
				}
				sort.Strings(got)
				rows[sh.Shard][src.Node] = got
			}
		}
		for shard, bySource := range rows {
			if len(bySource) < 2 {
				continue
			}
			multi++
			nodes := slices.Sorted(maps.Keys(bySource))
			for _, node := range nodes[1:] {
				gomega.ExpectWithOffset(1, bySource[node]).To(gomega.Equal(bySource[nodes[0]]), "%s/%s/%s shard %d: %s and %s hold the same rows",
					u.Catalog, u.Group, u.Segment, shard, nodes[0], node)
			}
		}
	}
	return multi
}
