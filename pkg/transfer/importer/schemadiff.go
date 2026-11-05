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
	"cmp"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	gocmp "github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/pkg/schema/topnschema"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

// Disposition is what the import does with one schema resource.
type Disposition string

// The schema dispositions.
const (
	DispositionOK        Disposition = "OK"
	DispositionCreate    Disposition = "CREATE"
	DispositionConflict  Disposition = "CONFLICT"
	DispositionIgnore    Disposition = "IGNORE"
	DispositionOverwrite Disposition = "OVERWRITE"
	DispositionInvalid   Disposition = "INVALID"
	DispositionMissing   Disposition = "MISSING"
)

// Blocking reports whether the disposition refuses the whole import.
func (d Disposition) Blocking() bool {
	return d == DispositionConflict || d == DispositionInvalid || d == DispositionMissing
}

// isWrite reports whether the disposition writes the artifact's spec.
func (d Disposition) isWrite() bool {
	return d == DispositionCreate || d == DispositionOverwrite
}

// DiffClass classifies a field difference.
type DiffClass string

// The difference classes.
const (
	ClassIdentity     DiffClass = "identity"
	ClassTopology     DiffClass = "topology"
	ClassShape        DiffClass = "shape"
	ClassBehavior     DiffClass = "behavior"
	ClassUnclassified DiffClass = "shape(unclassified)"
)

// SchemaOptions configure DiffSchemas.
type SchemaOptions struct {
	// Default is the policy (config.ConflictPolicy*) of resources no rule matches; empty means error.
	Default string
	// Rules override Default for the resources they match, the first match winning.
	Rules []config.ConflictRule
	// IncludeSchema false compares nothing: a resource the target lacks is MISSING. It is
	// false for an artifact exported without its schema.
	IncludeSchema bool
	// Force lets an overwrite change a group that holds data.
	Force bool
}

// FieldDiff is one differing field of a resource.
type FieldDiff struct {
	Path     string    `json:"path"`
	Class    DiffClass `json:"class"`
	Artifact string    `json:"artifact"`
	Target   string    `json:"target"`
}

// SchemaAction is the disposition of one schema resource of the artifact.
type SchemaAction struct {
	// Spec is the artifact's resource; nil for nothing to write.
	Spec        proto.Message `json:"-"`
	Kind        string        `json:"kind"`
	Group       string        `json:"group"`
	Name        string        `json:"name,omitempty"`
	Disposition Disposition   `json:"disposition"`
	Reason      string        `json:"reason,omitempty"`
	// Match is the rule match selecting this resource, pasteable into plan.yaml.
	Match string      `json:"match,omitempty"`
	Diffs []FieldDiff `json:"diffs,omitempty"`
	// Notes list the per-column consequences of writing the rows under the target schema, for
	// an IGNORE or a resource the import does not apply.
	Notes []string `json:"notes,omitempty"`
}

// ID renders the resource as <kind>/<group>[/<name>].
func (a *SchemaAction) ID() string {
	if a.Kind == schemasync.KindGroup {
		return a.Kind + "/" + a.Group
	}
	return a.Kind + "/" + a.Group + "/" + a.Name
}

// SchemaDiff is the result of DiffSchemas.
type SchemaDiff struct {
	// Actions are in dependency order, then group and name.
	Actions []SchemaAction
}

// Writes returns the CREATE and OVERWRITE actions in dependency order, each Spec cleared by
// PrepareForWrite.
func (d *SchemaDiff) Writes() []SchemaAction {
	var out []SchemaAction
	for _, a := range d.Actions {
		if a.Disposition.isWrite() {
			a.Spec = PrepareForWrite(a.Spec)
			out = append(out, a)
		}
	}
	return out
}

// kindOrder is the dependency order of schema writes.
func kindOrder(kind string) int { return slices.Index(schemasync.Kinds, kind) }

type resource struct {
	artifact proto.Message
	target   proto.Message
	kind     string
	group    string
	name     string
}

// DiffSchemas compares every artifact resource with the target and decides its disposition.
// measure/<group>/_top_n_result is left out of the comparison on both sides: the target
// creates it with its group, so it is OK, or a CONFLICT when its columns differ from the
// target's. art is the artifact's schema; target the target's.
func DiffSchemas(art *Schemas, target *TargetState, opts SchemaOptions) *SchemaDiff {
	if opts.Default == "" {
		opts.Default = config.ConflictPolicyError
	}
	d := &SchemaDiff{}
	// groupWrites holds the groups the import creates or overwrites; groups come first in
	// dependency order, so it is complete before any trace is decided.
	groupWrites := map[string]bool{}
	for _, r := range collectResources(art, &target.Schemas) {
		var a SchemaAction
		if r.kind == schemasync.KindMeasure && r.name == csvfmt.TopNResultMeasure {
			a = topNResultAction(r)
		} else {
			a = diffResource(r, art, target, opts, groupWrites)
		}
		d.Actions = append(d.Actions, a)
		if r.kind == schemasync.KindGroup && a.Disposition.isWrite() {
			groupWrites[r.group] = true
		}
	}
	return d
}

// collectResources pairs every artifact resource with its target counterpart, in dependency
// order, then group and name.
func collectResources(art, target *Schemas) []resource {
	var out []resource
	for name, g := range art.Groups {
		r := resource{kind: schemasync.KindGroup, group: name, artifact: g}
		if t, ok := target.Groups[name]; ok {
			r.target = t
		}
		out = append(out, r)
	}
	out = appendResources(out, schemasync.KindStream, art.Streams, target.Streams)
	out = appendResources(out, schemasync.KindMeasure, art.Measures, target.Measures)
	out = appendResources(out, schemasync.KindTrace, art.Traces, target.Traces)
	out = appendResources(out, schemasync.KindProperty, art.Properties, target.Properties)
	out = appendResources(out, schemasync.KindIndexRule, art.IndexRules, target.IndexRules)
	out = appendResources(out, schemasync.KindIndexRuleBinding, art.IndexRuleBindings, target.IndexRuleBindings)
	out = appendResources(out, schemasync.KindTopNAggregation, art.TopNs, target.TopNs)
	slices.SortFunc(out, func(a, b resource) int {
		return cmp.Or(cmp.Compare(kindOrder(a.kind), kindOrder(b.kind)), cmp.Compare(a.group, b.group), cmp.Compare(a.name, b.name))
	})
	return out
}

func appendResources[T proto.Message](out []resource, kind string, art, target map[ResourceKey]T) []resource {
	for k, v := range art {
		r := resource{kind: kind, group: k.Group, name: k.Name, artifact: v}
		if t, ok := target[k]; ok {
			r.target = t
		}
		out = append(out, r)
	}
	return out
}

func newAction(r resource) SchemaAction {
	a := SchemaAction{Kind: r.kind, Group: r.group, Name: r.name}
	a.Match = a.ID()
	return a
}

// topNResultAction checks that the artifact's _top_n_result has the columns of the target's:
// the target creates it with its group, so it is OK, never written, when they match. When the
// target has not created it yet, it is compared with the measure the target will create
// (topnschema.Measure, the generator the server uses); the recheck after the schema write
// sees the real one.
func topNResultAction(r resource) SchemaAction {
	a := newAction(r)
	a.Disposition = DispositionOK
	tm, predicted := r.target, false
	if tm == nil {
		tm, predicted = topnschema.Measure(topnschema.Metadata(r.group)), true
	}
	diffs := columnTypeDiffs(measureColumns(r.artifact.(*databasev1.Measure)), measureColumns(tm.(*databasev1.Measure)))
	if len(diffs) == 0 {
		return a
	}
	a.Disposition = DispositionConflict
	a.Diffs = diffs
	a.Reason = "the target's _top_n_result columns differ from the artifact's; its rows would be written in a foreign encoding"
	if predicted {
		a.Reason = "the _top_n_result the target creates with the group has columns that differ from the artifact's; its rows would be written in a foreign encoding"
	}
	return a
}

// columnTypeDiffs lists the columns whose presence or type differs between two column sets.
func columnTypeDiffs(art, target map[string]string) []FieldDiff {
	var out []FieldDiff
	for _, col := range unionKeys(art, target) {
		av, aok := art[col]
		tv, tok := target[col]
		if aok && tok && av == tv {
			continue
		}
		fd := FieldDiff{Path: col, Class: ClassShape, Artifact: noneValue, Target: noneValue}
		if aok {
			fd.Artifact = av
		}
		if tok {
			fd.Target = tv
		}
		out = append(out, fd)
	}
	return out
}

func unionKeys(a, b map[string]string) []string {
	all := make(map[string]string, len(a)+len(b))
	maps.Copy(all, a)
	maps.Copy(all, b)
	return slices.Sorted(maps.Keys(all))
}

// measureColumns maps "family.tag" and "_field.name" to the column type.
func measureColumns(m *databasev1.Measure) map[string]string {
	out := familyColumns(m.GetTagFamilies())
	for _, f := range m.GetFields() {
		out["_field."+f.GetName()] = f.GetFieldType().String()
	}
	return out
}

func familyColumns(families []*databasev1.TagFamilySpec) map[string]string {
	out := map[string]string{}
	for _, fam := range families {
		for _, t := range fam.GetTags() {
			out[fam.GetName()+"."+t.GetName()] = t.GetType().String()
		}
	}
	return out
}

type typedTag interface {
	GetName() string
	GetType() databasev1.TagType
}

func tagColumns[T typedTag](tags []T) map[string]string {
	out := map[string]string{}
	for _, t := range tags {
		out[t.GetName()] = t.GetType().String()
	}
	return out
}

// dataColumns returns the data columns of a stream, measure, trace or property, or nil for
// a kind without rows.
func dataColumns(m proto.Message) map[string]string {
	switch v := m.(type) {
	case *databasev1.Stream:
		return familyColumns(v.GetTagFamilies())
	case *databasev1.Measure:
		return measureColumns(v)
	case *databasev1.Trace:
		return tagColumns(v.GetTags())
	case *databasev1.Property:
		return tagColumns(v.GetTags())
	default:
		return nil
	}
}

func diffResource(r resource, art *Schemas, target *TargetState, opts SchemaOptions, groupWrites map[string]bool) SchemaAction {
	a := newAction(r)
	if !opts.IncludeSchema {
		return schemalessAction(a, r)
	}
	a.Spec = r.artifact
	var diffs []FieldDiff
	if r.target != nil {
		if diffs = DiffFields(r.kind, r.artifact, r.target); len(diffs) == 0 {
			a.Disposition, a.Spec = DispositionOK, nil
			return a
		}
		a.Diffs = diffs
	}
	conflict := func(reason string) SchemaAction {
		a.Disposition, a.Spec, a.Reason = DispositionConflict, nil, reason
		return a
	}
	// Validation only runs on a spec that is written: an IGNOREd spec never reaches the registry.
	invalid := func(err error) SchemaAction {
		a.Disposition, a.Spec, a.Reason = DispositionInvalid, nil, err.Error()
		return a
	}
	if r.target == nil {
		if err := validateResource(r, art, &target.Schemas); err != nil {
			return invalid(err)
		}
		if reason := traceSamplerRejection(r, art, &target.Schemas, groupWrites); reason != "" {
			return conflict("the registry would reject the create: " + reason)
		}
		a.Disposition = DispositionCreate
		return a
	}
	policy, source := resolvePolicy(opts, r.kind, r.group, r.name)
	if hasClass(diffs, ClassIdentity) {
		return conflict("an identity field differs, which no policy can resolve")
	}
	if r.kind == schemasync.KindMeasure && slices.ContainsFunc(diffs, func(d FieldDiff) bool {
		p := snakePath(d.Path)
		return p == "index_mode" || p == "interval"
	}) {
		return conflict("measure index_mode or interval differs, which no policy can resolve")
	}
	switch policy {
	case config.ConflictPolicyIgnore:
		notes, typeConflict := ignoreColumns(r)
		if typeConflict != "" {
			return conflict("policy=ignore " + source + ", but " + typeConflict)
		}
		a.Disposition, a.Spec, a.Notes = DispositionIgnore, nil, notes
		a.Reason = "policy=ignore " + source + "; the target schema stays as it is"
		return a
	case config.ConflictPolicyOverwrite:
		if err := validateResource(r, art, &target.Schemas); err != nil {
			return invalid(err)
		}
		if reason := updateRejection(r, art, &target.Schemas, groupWrites); reason != "" {
			return conflict("the registry would reject the update: " + reason)
		}
		if !opts.Force {
			if reason := overwriteEmptinessRejection(target, r.group); reason != "" {
				return conflict(reason)
			}
		}
		a.Disposition = DispositionOverwrite
		a.Reason = "policy=overwrite " + source
		return a
	default:
		if onlyTopologyOrBehavior(diffs) {
			return conflict("policy=error " + source + "; topology differences are expected for a cross-topology import, " +
				"set policy: ignore for this resource to accept them")
		}
		return conflict("policy=error " + source)
	}
}

// schemalessAction judges a resource when the import applies no schema (--include-schema=false).
func schemalessAction(a SchemaAction, r resource) SchemaAction {
	if r.target == nil {
		a.Disposition = DispositionMissing
		a.Reason = "the target has no such resource and the import does not create it (--include-schema=false, or the artifact has no schema)"
		return a
	}
	// The rows are still laid out and typed by an artifact schema the import does not apply,
	// so a column type or index_mode the target does not share would write wrong values.
	notes, typeConflict := ignoreColumns(r)
	if typeConflict != "" {
		a.Disposition, a.Reason = DispositionConflict, "--include-schema=false, but "+typeConflict
		return a
	}
	if am, ok := r.artifact.(*databasev1.Measure); ok && am.GetIndexMode() != r.target.(*databasev1.Measure).GetIndexMode() {
		a.Disposition, a.Reason = DispositionConflict, "--include-schema=false, but measure index_mode differs"
		return a
	}
	a.Disposition, a.Notes = DispositionOK, notes
	a.Reason = "not applied (--include-schema=false, or the artifact has no schema); the shared columns have the same types"
	return a
}

// overwriteEmptinessRejection returns why an overwrite without --force is refused, or "" when
// the group is known to be empty.
func overwriteEmptinessRejection(target *TargetState, group string) string {
	st, ok := target.NonEmptyGroups[group]
	switch {
	case !ok:
		return fmt.Sprintf("cannot prove group %s is empty (no Plan of it); overwrite needs --force", group)
	case !st.Known:
		return fmt.Sprintf("cannot prove group %s is empty (%s); overwrite needs --force", group, st.Reason)
	case st.NonEmpty:
		return fmt.Sprintf("group %s holds data; overwrite needs --force", group)
	}
	return ""
}

// onlyTopologyOrBehavior reports whether diffs hold a topology difference and nothing that
// changes the data shape.
func onlyTopologyOrBehavior(diffs []FieldDiff) bool {
	return hasClass(diffs, ClassTopology) &&
		!slices.ContainsFunc(diffs, func(d FieldDiff) bool { return d.Class != ClassTopology && d.Class != ClassBehavior })
}

func hasClass(diffs []FieldDiff, c DiffClass) bool {
	return slices.ContainsFunc(diffs, func(d FieldDiff) bool { return d.Class == c })
}

// resolvePolicy returns the policy of a resource: the first matching rule, else the default,
// with a parenthesized note of where it came from.
func resolvePolicy(opts SchemaOptions, kind, group, name string) (policy, source string) {
	for i, rule := range opts.Rules {
		if matchRule(rule.Match, kind, group, name) {
			return rule.Policy, fmt.Sprintf("(rules[%d] match: %s)", i, rule.Match)
		}
	}
	return opts.Default, "(default)"
}

func matchRule(match, kind, group, name string) bool {
	parts := strings.Split(match, "/")
	if len(parts) < 2 || parts[0] != kind || parts[1] != group {
		return false
	}
	if kind == schemasync.KindGroup {
		return len(parts) == 2
	}
	return len(parts) == 2 || (len(parts) == 3 && parts[2] == name)
}

// ignoreColumns lists what an IGNORE does to the data columns: artifact-only columns are
// dropped and target-only columns are filled with \N. A column both sides declare with
// different types cannot be imported, which the second result explains.
func ignoreColumns(r resource) ([]string, string) {
	art, tgt := dataColumns(r.artifact), dataColumns(r.target)
	var notes []string
	for _, col := range unionKeys(art, tgt) {
		av, aok := art[col]
		tv, tok := tgt[col]
		switch {
		case aok && tok && av != tv:
			return nil, fmt.Sprintf("column %s is %s in the artifact and %s on the target", col, av, tv)
		case aok && !tok:
			notes = append(notes, fmt.Sprintf("column %s: only in the artifact, dropped", col))
		case !aok && tok:
			notes = append(notes, fmt.Sprintf("column %s: only on the target, filled with \\N", col))
		}
	}
	return notes, ""
}

// validateResource re-runs the registry's create validation on an artifact resource and checks
// that the catalog of its group holds its kind.
func validateResource(r resource, art, target *Schemas) error {
	if err := validateSpec(r.artifact); err != nil {
		return fmt.Errorf("invalid %s: %w", r.kind, err)
	}
	if r.kind == schemasync.KindGroup {
		return nil
	}
	g, ok := art.Groups[r.group]
	if !ok {
		if g, ok = target.Groups[r.group]; !ok {
			return fmt.Errorf("group %s is neither in the artifact nor on the target", r.group)
		}
	}
	catalog := g.GetCatalog()
	var fits bool
	switch r.kind {
	case schemasync.KindStream:
		fits = catalog == commonv1.Catalog_CATALOG_STREAM
	case schemasync.KindMeasure, schemasync.KindTopNAggregation:
		fits = catalog == commonv1.Catalog_CATALOG_MEASURE
	case schemasync.KindTrace:
		fits = catalog == commonv1.Catalog_CATALOG_TRACE
	case schemasync.KindProperty:
		fits = catalog == commonv1.Catalog_CATALOG_PROPERTY
	default:
		fits = catalog != commonv1.Catalog_CATALOG_PROPERTY && catalog != commonv1.Catalog_CATALOG_UNSPECIFIED
	}
	if !fits {
		return fmt.Errorf("group %s has catalog %s, which cannot hold a %s", r.group, catalog, r.kind)
	}
	return nil
}

// validateSpec mirrors the validation the schema registry runs on create
// (banyand/metadata/schema/property/client.go).
func validateSpec(m proto.Message) error {
	switch v := m.(type) {
	case *commonv1.Group:
		return validate.Group(v)
	case *databasev1.Stream:
		if err := validate.Stream(v); err != nil {
			return err
		}
		return validate.UniqueTagNames(v.GetTagFamilies())
	case *databasev1.Measure:
		if v.GetInterval() != "" {
			if _, err := timestamp.ParseDuration(v.GetInterval()); err != nil {
				return fmt.Errorf("interval is malformed: %w", err)
			}
		}
		if err := validate.Measure(v); err != nil {
			return err
		}
		return validate.UniqueTagNames(v.GetTagFamilies())
	case *databasev1.Trace:
		return validate.Trace(v)
	case *databasev1.Property:
		return validate.Property(v)
	case *databasev1.IndexRule:
		if v.GetMetadata().GetId() == 0 {
			// The registry assigns a missing id before it validates.
			v = proto.Clone(v).(*databasev1.IndexRule)
			v.Metadata.Id = 1
		}
		return validate.IndexRule(v)
	case *databasev1.IndexRuleBinding:
		return validate.IndexRuleBinding(v)
	case *databasev1.TopNAggregation:
		return validate.TopNAggregation(v)
	default:
		return fmt.Errorf("unsupported resource %T", m)
	}
}

// updateRejection runs the checks the registry runs on Update, so an overwrite it would reject
// is a CONFLICT before anything is written. The sampler pipeline's trace count is re-implemented
// from checkPipelineAgainstTraces and checkTraceAgainstPipeline in
// banyand/metadata/schema/property/client.go; keep it in sync.
func updateRejection(r resource, art, target *Schemas, groupWrites map[string]bool) string {
	switch newSpec := r.artifact.(type) {
	case *databasev1.Stream:
		if err := validate.StreamUpdate(r.target.(*databasev1.Stream), newSpec); err != nil {
			return err.Error()
		}
	case *databasev1.Measure:
		if err := validate.MeasureUpdate(r.target.(*databasev1.Measure), newSpec); err != nil {
			return err.Error()
		}
	case *databasev1.Trace:
		if err := validate.TraceUpdate(r.target.(*databasev1.Trace), newSpec); err != nil {
			return err.Error()
		}
		return traceSamplerRejection(r, art, target, groupWrites)
	case *commonv1.Group:
		if validate.PipelineHasSampler(newSpec.GetPipeline()) {
			return samplerTraceCount(r.group, art, target)
		}
	}
	return ""
}

// traceSamplerRejection checks a trace the import writes against the sampler pipeline its group
// has once the import's group writes are applied: such a group may hold one trace only.
func traceSamplerRejection(r resource, art, target *Schemas, groupWrites map[string]bool) string {
	if r.kind != schemasync.KindTrace {
		return ""
	}
	g, ok := target.Groups[r.group]
	if !ok || groupWrites[r.group] {
		g = art.Groups[r.group]
	}
	if !validate.PipelineHasSampler(g.GetPipeline()) {
		return ""
	}
	return samplerTraceCount(r.group, art, target)
}

// samplerTraceCount rejects a sampler group that would hold more than one trace. Every artifact
// trace the target lacks is either created or blocks the import, so the union of both sides is
// what the group would hold.
func samplerTraceCount(group string, art, target *Schemas) string {
	names := map[string]struct{}{}
	for _, traces := range []map[ResourceKey]*databasev1.Trace{art.Traces, target.Traces} {
		for k := range traces {
			if k.Group == group {
				names[k.Name] = struct{}{}
			}
		}
	}
	if len(names) <= 1 {
		return ""
	}
	sorted := slices.Sorted(maps.Keys(names))
	return fmt.Sprintf("group %s has an enabled sampler pipeline but would hold %d traces (%s)", group, len(sorted), strings.Join(sorted, ", "))
}

// PrepareForWrite returns a copy of a schema resource without updated_at, created_at and
// Metadata.{create_revision, mod_revision}, so the registry stamps them anew; Metadata.id is
// kept, except an index rule's: it keys the rule's indexed fields, so the registry keeps the
// target's id on an update and derives it from group and name on a create.
func PrepareForWrite(m proto.Message) proto.Message {
	out := proto.Clone(m)
	if ir, ok := out.(*databasev1.IndexRule); ok && ir.GetMetadata() != nil {
		ir.Metadata.Id = 0
	}
	r := out.ProtoReflect()
	for _, name := range []protoreflect.Name{"updated_at", "created_at"} {
		if fd := r.Descriptor().Fields().ByName(name); fd != nil {
			r.Clear(fd)
		}
	}
	if fd := r.Descriptor().Fields().ByName("metadata"); fd != nil && r.Has(fd) {
		md := r.Mutable(fd).Message()
		for _, name := range []protoreflect.Name{"create_revision", "mod_revision"} {
			if f := md.Descriptor().Fields().ByName(name); f != nil {
				md.Clear(f)
			}
		}
	}
	return out
}

// DiffFields lists the differing fields of two resources of a kind, ignoring updated_at,
// created_at, Metadata.{id, create_revision, mod_revision} and unknown fields, each with its
// class. artifact and target must be of the same message type.
func DiffFields(kind string, artifact, target proto.Message) []FieldDiff {
	rep := &pathReporter{}
	gocmp.Equal(artifact, target,
		protocmp.Transform(),
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(artifact, "updated_at", "created_at"),
		protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
		gocmp.Reporter(rep),
	)
	for i := range rep.diffs {
		rep.diffs[i].Class = Classify(kind, snakePath(rep.diffs[i].Path))
	}
	return rep.diffs
}

const noneValue = "<none>"

// pathReporter collects the path and both values of every unequal leaf cmp reports.
type pathReporter struct {
	path  gocmp.Path
	diffs []FieldDiff
}

func (r *pathReporter) PushStep(ps gocmp.PathStep) { r.path = append(r.path, ps) }

func (r *pathReporter) PopStep() { r.path = r.path[:len(r.path)-1] }

func (r *pathReporter) Report(rs gocmp.Result) {
	if rs.Equal() {
		return
	}
	vx, vy := r.path.Last().Values()
	r.diffs = append(r.diffs, FieldDiff{Path: renderPath(r.path), Artifact: formatValue(vx), Target: formatValue(vy)})
}

// renderPath renders a cmp path over protocmp messages as "resourceOpts.stages[warm].ttl":
// field names in lower camel case, list elements by their name field when they have one.
func renderPath(p gocmp.Path) string {
	var b strings.Builder
	for i, step := range p {
		switch s := step.(type) {
		case gocmp.MapIndex:
			key := fmt.Sprint(s.Key().Interface())
			if i > 0 && p[i-1].Type() == messageType {
				if b.Len() > 0 {
					b.WriteByte('.')
				}
				b.WriteString(lowerCamel(key))
				continue
			}
			b.WriteString("[" + key + "]")
		case gocmp.SliceIndex:
			b.WriteString("[" + elementLabel(s) + "]")
		}
	}
	return b.String()
}

var messageType = reflect.TypeOf(protocmp.Message{})

// elementLabel names a list element by its name field, else by its index.
func elementLabel(s gocmp.SliceIndex) string {
	vx, vy := s.Values()
	for _, v := range []reflect.Value{vx, vy} {
		if !v.IsValid() {
			continue
		}
		if m, ok := v.Interface().(protocmp.Message); ok {
			if name, ok := m["name"].(string); ok && name != "" {
				return name
			}
		}
	}
	ix, iy := s.SplitKeys()
	if ix < 0 {
		return strconv.Itoa(iy)
	}
	return strconv.Itoa(ix)
}

func lowerCamel(snake string) string {
	parts := strings.Split(snake, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// snakePath turns a rendered path into the dotted proto field names without list elements,
// e.g. "resourceOpts.stages[warm].ttl" into "resource_opts.stages.ttl".
func snakePath(rendered string) string {
	var b strings.Builder
	depth := 0
	for i := 0; i < len(rendered); i++ {
		c := rendered[i]
		switch {
		case c == '[':
			depth++
		case c == ']':
			depth--
		case depth > 0:
		case c >= 'A' && c <= 'Z':
			b.WriteByte('_')
			b.WriteByte(c + 'a' - 'A')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// formatValue renders a compared value deterministically.
func formatValue(v reflect.Value) string {
	if !v.IsValid() {
		return noneValue
	}
	return formatAny(v.Interface())
}

func formatAny(x any) string {
	switch val := x.(type) {
	case protocmp.Message:
		keys := make([]string, 0, len(val))
		for k := range val {
			if !strings.HasPrefix(k, "@") {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, lowerCamel(k)+":"+formatAny(val[k]))
		}
		return "{" + strings.Join(parts, " ") + "}"
	case protocmp.Enum:
		return val.String()
	case string:
		return strconv.Quote(val)
	case []byte:
		return fmt.Sprintf("%x", val)
	default:
		rv := reflect.ValueOf(x)
		if rv.Kind() == reflect.Slice {
			parts := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				parts = append(parts, formatAny(rv.Index(i).Interface()))
			}
			return "[" + strings.Join(parts, " ") + "]"
		}
		if rv.Kind() == reflect.Map {
			keys := rv.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i].Interface()) < fmt.Sprint(keys[j].Interface()) })
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, fmt.Sprint(k.Interface())+":"+formatAny(rv.MapIndex(k).Interface()))
			}
			return "{" + strings.Join(parts, " ") + "}"
		}
		return fmt.Sprint(x)
	}
}

// classes maps "<kind>/<top-level field>" or "<kind>/<field>.<sub-field>" to its class.
var classes = map[string]DiffClass{
	"group/catalog":                        ClassIdentity,
	"indexRuleBinding/subject":             ClassIdentity,
	"topNAggregation/source_measure":       ClassIdentity,
	"group/resource_opts.shard_num":        ClassTopology,
	"group/resource_opts.segment_interval": ClassTopology,
	"group/resource_opts.ttl":              ClassTopology,
	"group/resource_opts.stages":           ClassTopology,
	"group/resource_opts.default_stages":   ClassTopology,
	"group/resource_opts.replicas":         ClassTopology,
	"group/pipeline":                       ClassBehavior,
	"measure/tag_families":                 ClassShape,
	"measure/fields":                       ClassShape,
	"measure/entity":                       ClassShape,
	"measure/sharding_key":                 ClassShape,
	"measure/index_mode":                   ClassShape,
	"measure/interval":                     ClassShape,
	"stream/tag_families":                  ClassShape,
	"stream/entity":                        ClassShape,
	"trace/tags":                           ClassShape,
	"trace/trace_id_tag_name":              ClassShape,
	"trace/timestamp_tag_name":             ClassShape,
	"trace/span_id_tag_name":               ClassShape,
	"indexRule/tags":                       ClassShape,
	"indexRule/type":                       ClassShape,
	"indexRule/analyzer":                   ClassShape,
	"indexRule/no_sort":                    ClassShape,
	"indexRuleBinding/rules":               ClassShape,
	"property/tags":                        ClassShape,
	"topNAggregation/field_name":           ClassBehavior,
	"topNAggregation/field_value_sort":     ClassBehavior,
	"topNAggregation/group_by_tag_names":   ClassBehavior,
	"topNAggregation/criteria":             ClassBehavior,
	"topNAggregation/counters_number":      ClassBehavior,
	"topNAggregation/lru_size":             ClassBehavior,
	"indexRuleBinding/begin_at":            ClassBehavior,
	"indexRuleBinding/expire_at":           ClassBehavior,
}

// Classify returns the class of a differing field: kind is a schemasync kind, path the dotted
// proto field names (e.g. "resource_opts.shard_num"). Metadata.{group, name} are identity;
// a field the table does not list is shape(unclassified).
func Classify(kind, path string) DiffClass {
	if path == "metadata.group" || path == "metadata.name" {
		return ClassIdentity
	}
	parts := strings.SplitN(path, ".", 3)
	if len(parts) >= 2 {
		if c, ok := classes[kind+"/"+parts[0]+"."+parts[1]]; ok {
			return c
		}
	}
	if c, ok := classes[kind+"/"+parts[0]]; ok {
		return c
	}
	return ClassUnclassified
}

// PredictState returns the target as it will be once the actions are applied: created and
// overwritten resources take the artifact's spec, a created group is known to be empty and
// every measure group without a _top_n_result gets the one the target creates with it.
// Routes and the collected issues are copied as they are.
func PredictState(target *TargetState, actions []SchemaAction) *TargetState {
	out := &TargetState{
		Schemas:        NewSchemas(),
		Routes:         target.Routes,
		RouteIssues:    target.RouteIssues,
		Blockers:       target.Blockers,
		Notes:          target.Notes,
		NonEmptyGroups: make(map[string]NonEmptyState, len(target.NonEmptyGroups)),
		DataNodes:      target.DataNodes,
		Standalone:     target.Standalone,
	}
	maps.Copy(out.Groups, target.Groups)
	maps.Copy(out.Streams, target.Streams)
	maps.Copy(out.Measures, target.Measures)
	maps.Copy(out.Traces, target.Traces)
	maps.Copy(out.Properties, target.Properties)
	maps.Copy(out.IndexRules, target.IndexRules)
	maps.Copy(out.IndexRuleBindings, target.IndexRuleBindings)
	maps.Copy(out.TopNs, target.TopNs)
	maps.Copy(out.NonEmptyGroups, target.NonEmptyGroups)
	for i := range actions {
		a := &actions[i]
		if a.Spec == nil || !a.Disposition.isWrite() {
			continue
		}
		_ = out.Add(a.Kind, a.Spec)
		if a.Kind == schemasync.KindGroup && a.Disposition == DispositionCreate {
			out.NonEmptyGroups[a.Group] = NonEmptyState{Known: true}
		}
	}
	for name, g := range out.Groups {
		k := ResourceKey{Group: name, Name: topnschema.Name}
		if _, ok := out.Measures[k]; !ok && g.GetCatalog() == commonv1.Catalog_CATALOG_MEASURE {
			out.Measures[k] = topnschema.Measure(topnschema.Metadata(name))
		}
	}
	return out
}
