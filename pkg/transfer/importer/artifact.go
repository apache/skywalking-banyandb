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

// Package importer imports a CSV or native artifact written by data export into a cluster: it checks
// the artifact, compares its schema with the target, decides which units and sources to push
// and renders that decision.
package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// ResourceKey identifies a group-scoped schema resource.
type ResourceKey struct {
	Group string
	Name  string
}

// Schemas holds schema resources: groups by name, every other kind by group and name.
type Schemas struct {
	Groups            map[string]*commonv1.Group
	Streams           map[ResourceKey]*databasev1.Stream
	Measures          map[ResourceKey]*databasev1.Measure
	Traces            map[ResourceKey]*databasev1.Trace
	Properties        map[ResourceKey]*databasev1.Property
	IndexRules        map[ResourceKey]*databasev1.IndexRule
	IndexRuleBindings map[ResourceKey]*databasev1.IndexRuleBinding
	TopNs             map[ResourceKey]*databasev1.TopNAggregation
}

// NewSchemas returns empty, non-nil schema maps.
func NewSchemas() Schemas {
	return Schemas{
		Groups:            map[string]*commonv1.Group{},
		Streams:           map[ResourceKey]*databasev1.Stream{},
		Measures:          map[ResourceKey]*databasev1.Measure{},
		Traces:            map[ResourceKey]*databasev1.Trace{},
		Properties:        map[ResourceKey]*databasev1.Property{},
		IndexRules:        map[ResourceKey]*databasev1.IndexRule{},
		IndexRuleBindings: map[ResourceKey]*databasev1.IndexRuleBinding{},
		TopNs:             map[ResourceKey]*databasev1.TopNAggregation{},
	}
}

// Add stores a schema resource of kind (a schemasync kind name) under its metadata.
func (s *Schemas) Add(kind string, m proto.Message) error {
	switch v := m.(type) {
	case *commonv1.Group:
		s.Groups[v.GetMetadata().GetName()] = v
	case *databasev1.Stream:
		s.Streams[keyOf(v.GetMetadata())] = v
	case *databasev1.Measure:
		s.Measures[keyOf(v.GetMetadata())] = v
	case *databasev1.Trace:
		s.Traces[keyOf(v.GetMetadata())] = v
	case *databasev1.Property:
		s.Properties[keyOf(v.GetMetadata())] = v
	case *databasev1.IndexRule:
		s.IndexRules[keyOf(v.GetMetadata())] = v
	case *databasev1.IndexRuleBinding:
		s.IndexRuleBindings[keyOf(v.GetMetadata())] = v
	case *databasev1.TopNAggregation:
		s.TopNs[keyOf(v.GetMetadata())] = v
	default:
		return fmt.Errorf("unsupported %s resource %T", kind, m)
	}
	return nil
}

func keyOf(md *commonv1.Metadata) ResourceKey {
	return ResourceKey{Group: md.GetGroup(), Name: md.GetName()}
}

// newMessage returns an empty message of a schemasync kind.
func newMessage(kind string) (proto.Message, error) {
	switch kind {
	case schemasync.KindGroup:
		return &commonv1.Group{}, nil
	case schemasync.KindStream:
		return &databasev1.Stream{}, nil
	case schemasync.KindMeasure:
		return &databasev1.Measure{}, nil
	case schemasync.KindTrace:
		return &databasev1.Trace{}, nil
	case schemasync.KindProperty:
		return &databasev1.Property{}, nil
	case schemasync.KindIndexRule:
		return &databasev1.IndexRule{}, nil
	case schemasync.KindIndexRuleBinding:
		return &databasev1.IndexRuleBinding{}, nil
	case schemasync.KindTopNAggregation:
		return &databasev1.TopNAggregation{}, nil
	default:
		return nil, fmt.Errorf("unknown schema kind %q", kind)
	}
}

// Artifact is an opened, checked CSV or native artifact with its decoded schema.
type Artifact struct {
	Manifest *manifest.Manifest
	Schemas
	Dir string
	// Warnings are the schema fields this importer does not know, found by a strict decode.
	Warnings []string
}

var (
	strictJSON  = protojson.UnmarshalOptions{}
	lenientJSON = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// Native reports whether the artifact holds native part files rather than CSV rows.
func (a *Artifact) Native() bool { return a.Manifest.Format == manifest.FormatNative }

// OpenArtifact reads and checks the artifact at dir: manifest.json must exist, the format must
// be csv or native and artifactVersion at most manifest.ArtifactVersion; every schema file must
// exist with the registered size and CRC-32, and every data file with the registered size and,
// when verifyData is set, CRC-32; the containers of a native artifact must open as complete
// volume sets with the registered entry counts, and with verifyData every volume is verified
// whole, member checksums included. Any failure is a preflight rejection. An artifact exported
// without its schema knows only its groups, from the manifest; its CSV columns are read with the
// target's schema. The schema files are decoded strictly first, so unknown fields become
// warnings, then leniently. A canceled ctx stops the checksum pass and maps to the exit code of
// the cancellation.
func OpenArtifact(ctx context.Context, dir string, verifyData bool) (*Artifact, error) {
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	if err != nil {
		return nil, exitcode.Exit(exitcode.ExitPreflight, "artifact %s: %v", dir, err)
	}
	if m.Format != manifest.FormatCSV && m.Format != manifest.FormatNative {
		return nil, exitcode.Exit(exitcode.ExitPreflight, "artifact %s has format %q; only csv and native can be imported", dir, m.Format)
	}
	if m.ArtifactVersion > manifest.ArtifactVersion {
		return nil, exitcode.Exit(exitcode.ExitPreflight, "artifact %s has artifactVersion %d; this bydbctl reads up to %d, upgrade it",
			dir, m.ArtifactVersion, manifest.ArtifactVersion)
	}
	a := &Artifact{Dir: dir, Manifest: m, Schemas: NewSchemas()}
	a.dropInternalGroups()
	if !a.HasSchema() {
		for _, g := range m.Groups {
			catalog, ok := transfer.CatalogByName(strings.ToLower(g.Catalog))
			if !ok {
				return nil, exitcode.Exit(exitcode.ExitPreflight, "artifact %s: group %s has unknown catalog %q", dir, g.Name, g.Catalog)
			}
			a.Groups[g.Name] = &commonv1.Group{Metadata: &commonv1.Metadata{Name: g.Name}, Catalog: catalog}
		}
	}
	for _, f := range m.SchemaFiles {
		if err = checkFile(ctx, dir, f, true); err != nil {
			return nil, err
		}
		if err = a.decodeSchemaFile(f.Path); err != nil {
			return nil, exitcode.Exit(exitcode.ExitPreflight, "artifact %s: %v", dir, err)
		}
	}
	if err = checkDataFiles(ctx, dir, m, verifyData); err != nil {
		return nil, err
	}
	if a.Native() {
		if err = checkContainers(ctx, dir, m, verifyData); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// dropInternalGroups leaves out the groups a server keeps for itself, which an older export
// included: every target has its own, so importing them would only collide with it.
func (a *Artifact) dropInternalGroups() {
	m := a.Manifest
	var dropped []string
	m.Groups = slices.DeleteFunc(m.Groups, func(g manifest.Group) bool {
		if common.IsInternalGroup(g.Name) {
			dropped = append(dropped, g.Name)
			return true
		}
		return false
	})
	m.Units = slices.DeleteFunc(m.Units, func(u manifest.Unit) bool { return common.IsInternalGroup(u.Group) })
	if len(dropped) > 0 {
		a.Warnings = append(a.Warnings, fmt.Sprintf("the artifact holds the server's internal group(s) %s; they are not imported", strings.Join(dropped, ", ")))
	}
}

// schemaGroup is the group a schema resource belongs to: a group's own name, otherwise the
// group of its metadata.
func schemaGroup(m proto.Message) string {
	if g, ok := m.(*commonv1.Group); ok {
		return g.GetMetadata().GetName()
	}
	if r, ok := m.(interface{ GetMetadata() *commonv1.Metadata }); ok {
		return r.GetMetadata().GetGroup()
	}
	return ""
}

// checkDataFiles checks every data file of the manifest, up to GOMAXPROCS at a time; the
// first failure stops the rest. The CRC-32 of a native volume is left to checkContainer, whose
// verification reads it once.
func checkDataFiles(ctx context.Context, dir string, m *manifest.Manifest, verifyData bool) error {
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	native := m.Format == manifest.FormatNative
	for files := range m.FileSets() {
		for _, f := range files {
			verify := verifyData && (!native || !strings.HasSuffix(f.Path, bnc.VolumeExt))
			eg.Go(func() error { return checkFile(egCtx, dir, f, verify) })
		}
	}
	return eg.Wait()
}

// checkContainers opens the volume footers of every container of a native artifact, up to
// GOMAXPROCS containers at a time, before anything is pushed: each volume must hold the entries
// the manifest registers, and the volumes of a container must make a complete set. An index
// stream completes a segment when it moves on, which cannot be undone, so a container that
// cannot be read must refuse the import before the first payload.
func checkContainers(ctx context.Context, dir string, m *manifest.Manifest, verifyData bool) error {
	var sets []map[string][]manifest.File
	for files := range m.FileSets() {
		containers, err := containerVolumes(files)
		if err != nil {
			return exitcode.Exit(exitcode.ExitPreflight, "%v", err)
		}
		sets = append(sets, containers)
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	for _, containers := range sets {
		for prefix, vols := range containers {
			eg.Go(func() error { return checkContainer(egCtx, dir, prefix, vols, verifyData) })
		}
	}
	return eg.Wait()
}

// checkContainer checks the volumes of one container, given in volume order: every volume must
// register its entry count, and with verifyData each volume is read once, its size, CRC-32 and
// entries compared with the manifest. A VolumeSet holds no more than one volume open.
func checkContainer(ctx context.Context, dir, prefix string, vols []manifest.File, verifyData bool) error {
	if err := ctx.Err(); err != nil {
		return exitcode.Exit(exitcode.For(err), "artifact %s: %w", dir, err)
	}
	// OpenVolumes rejects a missing, repeated or misplaced volume and a set without its last.
	vs, err := openVolumes(dir, vols)
	if err != nil {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact container %s: %v", prefix, err)
	}
	var sums []bnc.Summary
	if verifyData {
		sums, err = vs.Verify(ctx)
	}
	if closeErr := vs.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return exitcode.Exit(exitcode.For(ctxErr), "artifact %s: %w", dir, ctxErr)
		}
		return exitcode.Exit(exitcode.ExitPreflight, "artifact container %s: %v", prefix, err)
	}
	for i, f := range vols {
		if f.EntryCount == 0 {
			return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s registers no entryCount", f.Path)
		}
		if n := vs.VolumeEntries(i); n != int(f.EntryCount) {
			return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s has %d entries, the manifest registers %d", f.Path, n, f.EntryCount)
		}
		if sums == nil {
			continue
		}
		if got := manifest.FormatCRC32(sums[i].CRC32); sums[i].Bytes != f.Bytes || got != f.CRC32 {
			return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s has %d bytes and crc32 %s, the manifest registers %d and %s",
				f.Path, sums[i].Bytes, got, f.Bytes, f.CRC32)
		}
	}
	return nil
}

// checkFile verifies that a registered artifact file exists inside dir with its size and CRC-32.
// Manifest paths are slash-separated, so a backslash is rejected on every OS.
func checkFile(ctx context.Context, dir string, f manifest.File, verifyChecksum bool) error {
	if err := ctx.Err(); err != nil {
		return exitcode.Exit(exitcode.For(err), "artifact %s: %w", dir, err)
	}
	if f.Path == "" || strings.Contains(f.Path, "\\") || !filepath.IsLocal(filepath.FromSlash(f.Path)) {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file path %q escapes the artifact", f.Path)
	}
	full := filepath.Join(dir, filepath.FromSlash(f.Path))
	info, err := os.Stat(full)
	if err != nil {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s: %v", f.Path, err)
	}
	if uint64(info.Size()) != f.Bytes {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s has %d bytes, the manifest registers %d", f.Path, info.Size(), f.Bytes)
	}
	if !verifyChecksum {
		return nil
	}
	file, err := os.Open(full)
	if err != nil {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s: %v", f.Path, err)
	}
	defer file.Close()
	crc := crc32.NewIEEE()
	if _, err = io.Copy(crc, ctxReader{ctx: ctx, r: file}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return exitcode.Exit(exitcode.For(ctxErr), "artifact %s: %w", dir, ctxErr)
		}
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s: %v", f.Path, err)
	}
	if got := manifest.FormatCRC32(crc.Sum32()); got != f.CRC32 {
		return exitcode.Exit(exitcode.ExitPreflight, "artifact file %s has crc32 %s, the manifest registers %s", f.Path, got, f.CRC32)
	}
	return nil
}

// ctxReader fails its reads once ctx is done, so a long checksum pass can be interrupted.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// decodeSchemaFile decodes one schema/<kind>.jsonl file into the artifact's schemas.
func (a *Artifact) decodeSchemaFile(rel string) error {
	kind := strings.TrimSuffix(path.Base(rel), ".jsonl")
	if _, err := newMessage(kind); err != nil {
		return fmt.Errorf("schema file %s: %w", rel, err)
	}
	data, err := os.ReadFile(filepath.Join(a.Dir, filepath.FromSlash(rel)))
	if err != nil {
		return fmt.Errorf("read schema file %s: %w", rel, err)
	}
	lineNo := 0
	for raw := range bytes.Lines(data) {
		lineNo++
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		msg, _ := newMessage(kind)
		if strictErr := strictJSON.Unmarshal(line, msg); strictErr != nil {
			if err = lenientJSON.Unmarshal(line, msg); err != nil {
				return fmt.Errorf("schema file %s line %d: %w", rel, lineNo, err)
			}
			a.Warnings = append(a.Warnings, fmt.Sprintf("schema file %s line %d: fields unknown to this bydbctl were ignored: %v", rel, lineNo, strictErr))
		}
		if common.IsInternalGroup(schemaGroup(msg)) {
			continue
		}
		if err = a.Add(kind, msg); err != nil {
			return fmt.Errorf("schema file %s line %d: %w", rel, lineNo, err)
		}
	}
	return nil
}

// HasSchema tells whether the artifact was exported with its schema.
func (a *Artifact) HasSchema() bool {
	return len(a.Manifest.SchemaFiles) > 0
}

// Layout rebuilds the CSV column layout of a group from all of its resources, exactly as the
// exporting data node built it from the same schema.
func (s *Schemas) Layout(catalog commonv1.Catalog, group string) (*csvfmt.Layout, error) {
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		return csvfmt.StreamLayout(ofGroup(s.Streams, group))
	case commonv1.Catalog_CATALOG_MEASURE:
		return csvfmt.MeasureLayout(ofGroup(s.Measures, group))
	case commonv1.Catalog_CATALOG_TRACE:
		return csvfmt.TraceLayout(ofGroup(s.Traces, group))
	case commonv1.Catalog_CATALOG_PROPERTY:
		return csvfmt.PropertyLayout(ofGroup(s.Properties, group))
	default:
		return nil, errors.New("unsupported catalog " + catalog.String())
	}
}

// ofGroup returns the resources of a group; the layout builders sort them by name.
func ofGroup[T proto.Message](m map[ResourceKey]T, group string) []T {
	var out []T
	for k, v := range m {
		if k.Group == group {
			out = append(out, v)
		}
	}
	return out
}
