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

// Package schemasync exports the schema of the selected groups through the liaison's
// registry services into the artifact's schema/<kind>.jsonl files.
package schemasync

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

// Dir is the schema directory below the artifact root.
const Dir = "schema"

// The schema kinds, which are also the file base names below Dir.
const (
	KindGroup            = "group"
	KindStream           = "stream"
	KindMeasure          = "measure"
	KindTrace            = "trace"
	KindProperty         = "property"
	KindIndexRule        = "indexRule"
	KindIndexRuleBinding = "indexRuleBinding"
	KindTopNAggregation  = "topNAggregation"
)

// Kinds lists every schema kind in file order.
var Kinds = []string{KindGroup, KindStream, KindMeasure, KindTrace, KindProperty, KindIndexRule, KindIndexRuleBinding, KindTopNAggregation}

// File is one written schema file.
type File struct {
	// Path is relative to the artifact root, slash separated: "schema/<kind>.jsonl".
	Path  string
	Bytes uint64
	CRC32 uint32
}

type resourceKey struct {
	group, name string
	catalog     commonv1.Catalog
}

// Result is what Export wrote, with the decoded measures the exporter needs afterwards.
type Result struct {
	revisions    map[resourceKey]int64
	fingerprints map[string]string
	Measures     []*databasev1.Measure
	Files        []File
}

// GroupRevisions returns the mod revision the registry reported for every stream, measure,
// trace or property resource of a group, keyed by resource name.
func (r *Result) GroupRevisions(catalog commonv1.Catalog, group string) map[string]int64 {
	out := map[string]int64{}
	for key, rev := range r.revisions {
		if key.catalog == catalog && key.group == group {
			out[key.name] = rev
		}
	}
	return out
}

// Export lists every resource of groups through conn and writes one protojson line per
// resource, unpopulated fields included, to dir/schema/<kind>.jsonl. Only the kinds a group's
// catalog can hold are listed. Every kind gets a file, possibly empty, so the artifact's
// file set does not depend on the data. Files are 0600 and the directory 0700.
func Export(ctx context.Context, conn *grpc.ClientConn, groups []*commonv1.Group, dir string) (*Result, error) {
	res := &Result{revisions: map[resourceKey]int64{}, fingerprints: map[string]string{}}
	lines := map[string][]proto.Message{KindGroup: nil}
	for _, g := range groups {
		lines[KindGroup] = append(lines[KindGroup], g)
		res.addFingerprint(KindGroup, g)
		catalog := g.GetCatalog()
		if err := ListGroup(ctx, conn, g, func(kind string, m proto.Message) error {
			lines[kind] = append(lines[kind], m)
			res.record(kind, catalog, m)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	schemaDir := filepath.Join(dir, Dir)
	if err := resume.MkdirAll(schemaDir, 0o700); err != nil {
		return nil, fmt.Errorf("create schema directory: %w", err)
	}
	for _, kind := range Kinds {
		// Registries list in no fixed order; sorting keeps the files byte-identical for the same schema.
		slices.SortFunc(lines[kind], compareByMetadata)
		f, err := writeKind(schemaDir, kind, lines[kind])
		if err != nil {
			return nil, err
		}
		res.Files = append(res.Files, f)
	}
	if err := fs.SyncDirIfSupported(schemaDir); err != nil {
		return nil, err
	}
	return res, nil
}

// Fingerprints maps every resource of the schema, keyed "<kind> <group>/<name>" ("group <name>"
// for a group), to what tells its versions apart: "revision <n>", its metadata's mod revision,
// which the registry sets on every create and every update that changes it, of any kind. Two
// schemas with the same fingerprints are the same schema. The content itself is not compared: a
// resource read back from a schema file lacks the fields this bydbctl does not know, which the
// one read from the registry keeps.
func (r *Result) Fingerprints() map[string]string { return r.fingerprints }

// Live lists every resource of groups through conn, as Export does, without writing anything,
// and returns their Fingerprints.
func Live(ctx context.Context, conn *grpc.ClientConn, groups []*commonv1.Group) (map[string]string, error) {
	res := &Result{fingerprints: map[string]string{}}
	for _, g := range groups {
		res.addFingerprint(KindGroup, g)
		if err := ListGroup(ctx, conn, g, func(kind string, m proto.Message) error {
			res.addFingerprint(kind, m)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return res.fingerprints, nil
}

// DiffFingerprints spells, sorted, every resource whose fingerprint differs between recorded and
// live, or that only one of them holds; nil when they are equal.
func DiffFingerprints(recorded, live map[string]string) []string {
	var out []string
	for k, v := range recorded {
		w, ok := live[k]
		switch {
		case !ok:
			out = append(out, k+": removed")
		case v != w:
			out = append(out, fmt.Sprintf("%s: recorded %s, now %s", k, v, w))
		}
	}
	for k := range live {
		if _, ok := recorded[k]; !ok {
			out = append(out, k+": added")
		}
	}
	slices.Sort(out)
	return out
}

// DataKinds are the kinds of the resources a CSV file binds its columns to: a node encodes a
// unit of a group with every resource of these kinds in the group and reports their mod revisions.
// Index rules, their bindings, Top-N aggregations and the group itself are not among them.
var DataKinds = []string{KindStream, KindMeasure, KindTrace, KindProperty}

// Select keeps the fingerprints of the resources of kinds that belong to groups.
func Select(fingerprints map[string]string, kinds, groups []string) map[string]string {
	out := map[string]string{}
	for k, v := range fingerprints {
		kind, rest, ok := strings.Cut(k, " ")
		if !ok || !slices.Contains(kinds, kind) {
			continue
		}
		group, _, _ := strings.Cut(rest, "/")
		if slices.Contains(groups, group) {
			out[k] = v
		}
	}
	return out
}

// Restrict splits live into the fingerprints of the resources recorded holds and, sorted, the
// keys of those it lacks: the resources added since recorded was taken.
func Restrict(live, recorded map[string]string) (known map[string]string, added []string) {
	known = make(map[string]string, len(recorded))
	for k, v := range live {
		if _, ok := recorded[k]; ok {
			known[k] = v
		} else {
			added = append(added, k)
		}
	}
	slices.Sort(added)
	return known, added
}

// record adds a resource of kind in a group of catalog: its fingerprint, the mod revision of a
// stream, measure, trace or property, and a measure itself.
func (r *Result) record(kind string, catalog commonv1.Catalog, m proto.Message) {
	r.addFingerprint(kind, m)
	switch kind {
	case KindStream, KindMeasure, KindTrace, KindProperty:
		md := m.(interface{ GetMetadata() *commonv1.Metadata }).GetMetadata()
		r.revisions[resourceKey{catalog: catalog, group: md.GetGroup(), name: md.GetName()}] = md.GetModRevision()
	}
	if v, ok := m.(*databasev1.Measure); ok {
		r.Measures = append(r.Measures, v)
	}
}

func (r *Result) addFingerprint(kind string, m proto.Message) {
	md := m.(interface{ GetMetadata() *commonv1.Metadata }).GetMetadata()
	r.fingerprints[kind+" "+path.Join(md.GetGroup(), md.GetName())] = fmt.Sprintf("revision %d", md.GetModRevision())
}

// compareByMetadata orders schema messages by group, then name.
func compareByMetadata(a, b proto.Message) int {
	ma := a.(interface{ GetMetadata() *commonv1.Metadata }).GetMetadata()
	mb := b.(interface{ GetMetadata() *commonv1.Metadata }).GetMetadata()
	return cmp.Or(cmp.Compare(ma.GetGroup(), mb.GetGroup()), cmp.Compare(ma.GetName(), mb.GetName()))
}

// ListGroup lists every resource of a group through conn, only the kinds its catalog can hold,
// and calls visit with the kind and the resource; the group itself is not visited. A listing or
// visit failure stops it.
func ListGroup(ctx context.Context, conn *grpc.ClientConn, g *commonv1.Group, visit func(kind string, m proto.Message) error) error {
	name := g.GetMetadata().GetName()
	wrap := func(kind string, err error) error {
		return fmt.Errorf("list %s of group %s: %w", kind, name, err)
	}
	switch g.GetCatalog() {
	case commonv1.Catalog_CATALOG_STREAM:
		resp, err := databasev1.NewStreamRegistryServiceClient(conn).List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindStream, err)
		}
		if err = visitAll(KindStream, resp.GetStream(), visit); err != nil {
			return err
		}
	case commonv1.Catalog_CATALOG_MEASURE:
		resp, err := databasev1.NewMeasureRegistryServiceClient(conn).List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindMeasure, err)
		}
		if err = visitAll(KindMeasure, resp.GetMeasure(), visit); err != nil {
			return err
		}
		topN, err := databasev1.NewTopNAggregationRegistryServiceClient(conn).List(ctx, &databasev1.TopNAggregationRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindTopNAggregation, err)
		}
		if err = visitAll(KindTopNAggregation, topN.GetTopNAggregation(), visit); err != nil {
			return err
		}
	case commonv1.Catalog_CATALOG_TRACE:
		resp, err := databasev1.NewTraceRegistryServiceClient(conn).List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindTrace, err)
		}
		if err = visitAll(KindTrace, resp.GetTrace(), visit); err != nil {
			return err
		}
	case commonv1.Catalog_CATALOG_PROPERTY:
		resp, err := databasev1.NewPropertyRegistryServiceClient(conn).List(ctx, &databasev1.PropertyRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindProperty, err)
		}
		return visitAll(KindProperty, resp.GetProperties(), visit) // property groups carry no index rules
	default:
		return fmt.Errorf("group %s has unsupported catalog %s", name, g.GetCatalog())
	}
	rules, err := databasev1.NewIndexRuleRegistryServiceClient(conn).List(ctx, &databasev1.IndexRuleRegistryServiceListRequest{Group: name})
	if err != nil {
		return wrap(KindIndexRule, err)
	}
	if err = visitAll(KindIndexRule, rules.GetIndexRule(), visit); err != nil {
		return err
	}
	bindings, err := databasev1.NewIndexRuleBindingRegistryServiceClient(conn).List(ctx, &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: name})
	if err != nil {
		return wrap(KindIndexRuleBinding, err)
	}
	return visitAll(KindIndexRuleBinding, bindings.GetIndexRuleBinding(), visit)
}

func visitAll[T proto.Message](kind string, msgs []T, visit func(kind string, m proto.Message) error) error {
	for _, m := range msgs {
		if err := visit(kind, m); err != nil {
			return err
		}
	}
	return nil
}

var marshalOptions = protojson.MarshalOptions{EmitUnpopulated: true}

// writeKind writes one JSON Lines file and fsyncs it. On any error the partial file is
// removed, so a failed export leaves no half-written schema file behind.
func writeKind(schemaDir, kind string, msgs []proto.Message) (out File, err error) {
	name := kind + ".jsonl"
	out = File{Path: path.Join(Dir, name)}
	target := filepath.Join(schemaDir, name)
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return out, fmt.Errorf("create %s: %w", out.Path, err)
	}
	closed := false
	defer func() {
		if err == nil {
			return
		}
		if !closed {
			_ = f.Close()
		}
		_ = os.Remove(target)
	}()
	crc := crc32.NewIEEE()
	w := bufio.NewWriter(f)
	for _, m := range msgs {
		line, marshalErr := marshalOptions.Marshal(m)
		if marshalErr != nil {
			return out, fmt.Errorf("encode %s: %w", out.Path, marshalErr)
		}
		line = append(line, '\n')
		_, _ = crc.Write(line)
		out.Bytes += uint64(len(line))
		if _, err = w.Write(line); err != nil {
			return out, fmt.Errorf("write %s: %w", out.Path, err)
		}
	}
	if err = w.Flush(); err == nil {
		err = f.Sync()
	}
	closed = true
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return out, fmt.Errorf("write %s: %w", out.Path, err)
	}
	out.CRC32 = crc.Sum32()
	return out, nil
}
