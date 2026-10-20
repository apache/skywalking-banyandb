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
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
	revisions map[resourceKey]int64
	Measures  []*databasev1.Measure
	Files     []File
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
	res := &Result{revisions: map[resourceKey]int64{}}
	lines := map[string][]proto.Message{KindGroup: nil}
	for _, g := range groups {
		lines[KindGroup] = append(lines[KindGroup], g)
		if err := listGroup(ctx, conn, g, res, lines); err != nil {
			return nil, err
		}
	}
	schemaDir := filepath.Join(dir, Dir)
	if err := os.MkdirAll(schemaDir, 0o700); err != nil {
		return nil, fmt.Errorf("create schema directory: %w", err)
	}
	for _, kind := range Kinds {
		f, err := writeKind(schemaDir, kind, lines[kind])
		if err != nil {
			return nil, err
		}
		res.Files = append(res.Files, f)
	}
	if err := syncDir(schemaDir); err != nil {
		return nil, err
	}
	return res, nil
}

// syncDir makes the directory's new entries durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	return nil
}

func listGroup(ctx context.Context, conn *grpc.ClientConn, g *commonv1.Group, res *Result, lines map[string][]proto.Message) error {
	name := g.GetMetadata().GetName()
	catalog := g.GetCatalog()
	record := func(md *commonv1.Metadata) {
		res.revisions[resourceKey{catalog: catalog, group: md.GetGroup(), name: md.GetName()}] = md.GetModRevision()
	}
	wrap := func(kind string, err error) error {
		return fmt.Errorf("list %s of group %s: %w", kind, name, err)
	}
	switch catalog {
	case commonv1.Catalog_CATALOG_STREAM:
		resp, err := databasev1.NewStreamRegistryServiceClient(conn).List(ctx, &databasev1.StreamRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindStream, err)
		}
		for _, s := range resp.GetStream() {
			record(s.GetMetadata())
			lines[KindStream] = append(lines[KindStream], s)
		}
	case commonv1.Catalog_CATALOG_MEASURE:
		resp, err := databasev1.NewMeasureRegistryServiceClient(conn).List(ctx, &databasev1.MeasureRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindMeasure, err)
		}
		for _, m := range resp.GetMeasure() {
			record(m.GetMetadata())
			res.Measures = append(res.Measures, m)
			lines[KindMeasure] = append(lines[KindMeasure], m)
		}
		topN, err := databasev1.NewTopNAggregationRegistryServiceClient(conn).List(ctx, &databasev1.TopNAggregationRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindTopNAggregation, err)
		}
		for _, t := range topN.GetTopNAggregation() {
			lines[KindTopNAggregation] = append(lines[KindTopNAggregation], t)
		}
	case commonv1.Catalog_CATALOG_TRACE:
		resp, err := databasev1.NewTraceRegistryServiceClient(conn).List(ctx, &databasev1.TraceRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindTrace, err)
		}
		for _, t := range resp.GetTrace() {
			record(t.GetMetadata())
			lines[KindTrace] = append(lines[KindTrace], t)
		}
	case commonv1.Catalog_CATALOG_PROPERTY:
		resp, err := databasev1.NewPropertyRegistryServiceClient(conn).List(ctx, &databasev1.PropertyRegistryServiceListRequest{Group: name})
		if err != nil {
			return wrap(KindProperty, err)
		}
		for _, p := range resp.GetProperties() {
			record(p.GetMetadata())
			lines[KindProperty] = append(lines[KindProperty], p)
		}
		return nil // property groups carry no index rules
	default:
		return fmt.Errorf("group %s has unsupported catalog %s", name, catalog)
	}
	rules, err := databasev1.NewIndexRuleRegistryServiceClient(conn).List(ctx, &databasev1.IndexRuleRegistryServiceListRequest{Group: name})
	if err != nil {
		return wrap(KindIndexRule, err)
	}
	for _, r := range rules.GetIndexRule() {
		lines[KindIndexRule] = append(lines[KindIndexRule], r)
	}
	bindings, err := databasev1.NewIndexRuleBindingRegistryServiceClient(conn).List(ctx, &databasev1.IndexRuleBindingRegistryServiceListRequest{Group: name})
	if err != nil {
		return wrap(KindIndexRuleBinding, err)
	}
	for _, b := range bindings.GetIndexRuleBinding() {
		lines[KindIndexRuleBinding] = append(lines[KindIndexRuleBinding], b)
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
