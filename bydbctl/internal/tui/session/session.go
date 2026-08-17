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

// Package session defines the durable state shared by the bydbctl agent TUI workflow and agent adapters.
package session

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Phase is the deterministic workflow phase owned by bydbctl.
type Phase string

// Workflow phases.
const (
	PhaseIntent       Phase = "intent"
	PhaseAgentDraft   Phase = "agent_draft"
	PhaseConversation Phase = "conversation"
	PhaseClarifying   Phase = "clarifying"
	PhaseValidate     Phase = "validate"
	PhaseReady        Phase = "ready"
	PhaseExecuted     Phase = "executed"
	PhaseError        Phase = "error"
	// PhaseSchema is a turn answered from the schema catalog, without any BYDBQL candidate.
	PhaseSchema Phase = "schema"
)

// String returns the phase name.
func (p Phase) String() string {
	return string(p)
}

// ResourceType identifies the BanyanDB resource targeted by a BYDBQL query.
type ResourceType string

// Supported resource types.
const (
	ResourceTypeMeasure  ResourceType = "MEASURE"
	ResourceTypeStream   ResourceType = "STREAM"
	ResourceTypeTrace    ResourceType = "TRACE"
	ResourceTypeProperty ResourceType = "PROPERTY"
	ResourceTypeTopN     ResourceType = "TOPN"
)

// NormalizeResourceType converts user input into a supported resource type.
func NormalizeResourceType(input string) ResourceType {
	switch strings.ToUpper(strings.TrimSpace(input)) {
	case string(ResourceTypeStream):
		return ResourceTypeStream
	case string(ResourceTypeTrace):
		return ResourceTypeTrace
	case string(ResourceTypeProperty):
		return ResourceTypeProperty
	case string(ResourceTypeTopN), "TOP-N", "TOP_N":
		return ResourceTypeTopN
	default:
		return ResourceTypeMeasure
	}
}

// String returns the resource type name.
func (rt ResourceType) String() string {
	return string(rt)
}

// TimeRange stores raw BYDBQL-compatible time bounds.
type TimeRange struct {
	Start string
	End   string
}

// SchemaColumnKind identifies how a column is represented by a BanyanDB schema.
type SchemaColumnKind string

// Schema column kinds.
const (
	SchemaColumnTag       SchemaColumnKind = "tag"
	SchemaColumnEntityTag SchemaColumnKind = "entity_tag"
	SchemaColumnField     SchemaColumnKind = "field"
)

// SchemaValueType identifies the queryable type of a schema column.
type SchemaValueType string

// Schema value types.
const (
	SchemaValueTypeUnknown     SchemaValueType = "unknown"
	SchemaValueTypeString      SchemaValueType = "string"
	SchemaValueTypeInt         SchemaValueType = "int"
	SchemaValueTypeFloat       SchemaValueType = "float"
	SchemaValueTypeStringArray SchemaValueType = "string_array"
	SchemaValueTypeIntArray    SchemaValueType = "int_array"
	SchemaValueTypeTimestamp   SchemaValueType = "timestamp"
	SchemaValueTypeBinary      SchemaValueType = "binary"
)

// SchemaColumn is one typed, queryable column from a resource schema.
type SchemaColumn struct {
	Name    string
	Kind    SchemaColumnKind
	Type    SchemaValueType
	Indexed bool
}

// SortableIndex describes an index rule that may be used by ORDER BY.
type SortableIndex struct {
	RuleName string
	Tags     []string
}

// SchemaSnapshot is the schema summary passed across the agent boundary.
type SchemaSnapshot struct {
	UpdatedAt          time.Time
	Type               ResourceType
	Name               string
	Groups             []string
	Tags               []string
	EntityTags         []string
	Fields             []string
	Columns            []SchemaColumn
	IndexedFields      []string
	SortableIndexes    []SortableIndex
	SourceMeasure      string
	SourceMeasureGroup string
	FieldValueSort     string
	Fingerprint        string
	// TraceIDTag names the TRACE tag that identifies a trace, which is the only tag whose equality
	// filter lets a TRACE query run without ORDER BY.
	TraceIDTag string
	// TimestampTag names the TRACE tag that carries the span timestamp.
	TimestampTag    string
	ResourceNames   []string
	AvailableGroups []string
	Catalog         []CatalogEntry
	Loaded          bool
}

// conventionalTraceIDTag is the trace-ID tag name used by the trace schemas BanyanDB ships.
const conventionalTraceIDTag = "trace_id"

// TraceIDTagName reports the tag that identifies a trace, or "" when the schema has none.
//
// A schema loaded before TraceIDTag existed, or merged across groups that disagree, leaves the field
// empty, so the conventional name is matched against both the tag list and the typed columns.
func (snapshot *SchemaSnapshot) TraceIDTagName() string {
	if snapshot == nil {
		return ""
	}
	if traceIDTag := strings.TrimSpace(snapshot.TraceIDTag); traceIDTag != "" {
		return traceIDTag
	}
	for _, tagName := range snapshot.Tags {
		if strings.EqualFold(strings.TrimSpace(tagName), conventionalTraceIDTag) {
			return strings.TrimSpace(tagName)
		}
	}
	for _, column := range snapshot.Columns {
		if strings.EqualFold(strings.TrimSpace(column.Name), conventionalTraceIDTag) {
			return strings.TrimSpace(column.Name)
		}
	}
	return ""
}

// EnsureFingerprint computes a deterministic schema identity when one is not already set.
func (snapshot *SchemaSnapshot) EnsureFingerprint() string {
	if snapshot == nil {
		return ""
	}
	if snapshot.Fingerprint != "" {
		return snapshot.Fingerprint
	}
	groups := append([]string(nil), snapshot.Groups...)
	sort.Strings(groups)
	tags := append([]string(nil), snapshot.Tags...)
	sort.Strings(tags)
	entityTags := append([]string(nil), snapshot.EntityTags...)
	sort.Strings(entityTags)
	fields := append([]string(nil), snapshot.Fields...)
	sort.Strings(fields)
	columns := append([]SchemaColumn(nil), snapshot.Columns...)
	sort.Slice(columns, func(leftIndex, rightIndex int) bool {
		if columns[leftIndex].Name != columns[rightIndex].Name {
			return columns[leftIndex].Name < columns[rightIndex].Name
		}
		if columns[leftIndex].Kind != columns[rightIndex].Kind {
			return columns[leftIndex].Kind < columns[rightIndex].Kind
		}
		return columns[leftIndex].Type < columns[rightIndex].Type
	})
	indexes := CloneSortableIndexes(snapshot.SortableIndexes)
	for indexPosition := range indexes {
		sort.Strings(indexes[indexPosition].Tags)
	}
	sort.Slice(indexes, func(leftIndex, rightIndex int) bool {
		return indexes[leftIndex].RuleName < indexes[rightIndex].RuleName
	})
	var fingerprintSource strings.Builder
	fingerprintSource.WriteString(snapshot.Type.String())
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(snapshot.Name)
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(strings.Join(groups, ","))
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(strings.Join(tags, ","))
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(strings.Join(entityTags, ","))
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(strings.Join(fields, ","))
	fingerprintSource.WriteByte('\n')
	for _, column := range columns {
		fingerprintSource.WriteString(column.Name)
		fingerprintSource.WriteByte(':')
		fingerprintSource.WriteString(string(column.Kind))
		fingerprintSource.WriteByte(':')
		fingerprintSource.WriteString(string(column.Type))
		if column.Indexed {
			fingerprintSource.WriteByte(':')
			fingerprintSource.WriteString("indexed")
		}
		fingerprintSource.WriteByte('\n')
	}
	for _, index := range indexes {
		fingerprintSource.WriteString(index.RuleName)
		fingerprintSource.WriteByte(':')
		fingerprintSource.WriteString(strings.Join(index.Tags, ","))
		fingerprintSource.WriteByte('\n')
	}
	fingerprintSource.WriteString(snapshot.SourceMeasure)
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(snapshot.SourceMeasureGroup)
	fingerprintSource.WriteByte('\n')
	fingerprintSource.WriteString(snapshot.FieldValueSort)
	digest := sha256.Sum256([]byte(fingerprintSource.String()))
	snapshot.Fingerprint = hex.EncodeToString(digest[:])
	return snapshot.Fingerprint
}

// Column returns a typed schema column by its case-insensitive name.
func (snapshot SchemaSnapshot) Column(name string) (SchemaColumn, bool) {
	trimmedName := strings.TrimSpace(name)
	for _, column := range snapshot.Columns {
		if strings.EqualFold(column.Name, trimmedName) {
			return column, true
		}
	}
	var suffixMatch SchemaColumn
	matchCount := 0
	for _, column := range snapshot.Columns {
		if strings.EqualFold(column.Name[strings.LastIndex(column.Name, ".")+1:], trimmedName) {
			suffixMatch = column
			matchCount++
		}
	}
	if matchCount == 1 {
		return suffixMatch, true
	}
	return SchemaColumn{}, false
}

// ExactColumn resolves a schema column without changing identifier case.
func (snapshot SchemaSnapshot) ExactColumn(name string) (SchemaColumn, bool) {
	trimmedName := strings.TrimSpace(name)
	for _, column := range snapshot.Columns {
		if column.Name == trimmedName {
			return column, true
		}
	}
	var suffixMatch SchemaColumn
	matchCount := 0
	for _, column := range snapshot.Columns {
		if column.Name[strings.LastIndex(column.Name, ".")+1:] == trimmedName {
			suffixMatch = column
			matchCount++
		}
	}
	if matchCount == 1 {
		return suffixMatch, true
	}
	return SchemaColumn{}, false
}

// CatalogEntry is one discoverable BanyanDB resource in a group.
type CatalogEntry struct {
	Group string
	Type  ResourceType
	Name  string
}

// SchemaCatalog is the full read-only resource catalog discovered from BanyanDB.
type SchemaCatalog struct {
	UpdatedAt time.Time
	Groups    []string
	Entries   []CatalogEntry
}

// CandidateSource records where a BYDBQL candidate came from.
type CandidateSource string

// ChatRole identifies who authored a chat message.
type ChatRole string

// Chat roles.
const (
	ChatRoleUser      ChatRole = "user"
	ChatRoleAssistant ChatRole = "assistant"
	ChatRoleTool      ChatRole = "tool"
	ChatRoleSystem    ChatRole = "system"
)

// ChatMessageKind distinguishes what an assistant message is asking of the user.
type ChatMessageKind string

// Chat message kinds. An empty kind carries no extra meaning.
const (
	// ChatMessageKindAnswer is a reply that completes the turn without proposing a query.
	ChatMessageKindAnswer ChatMessageKind = "answer"
	// ChatMessageKindClarification is a reply that waits on the user before any query can be built.
	ChatMessageKindClarification ChatMessageKind = "clarification"
	// ChatMessageKindSchema is a resource description read straight from the schema catalog.
	ChatMessageKindSchema ChatMessageKind = "schema"
)

// ChatMessage is one user-visible chat entry in the agent conversation.
type ChatMessage struct {
	CreatedAt  time.Time
	Validation *ValidationReport
	Role       ChatRole
	Kind       ChatMessageKind
	Content    string
	Detail     string
	Candidate  string
	ToolName   string
}

// Candidate sources.
const (
	CandidateSourceAgent  CandidateSource = "agent"
	CandidateSourceManual CandidateSource = "manual"
)

// BydbqlCandidate is a versioned candidate query and its validation state.
type BydbqlCandidate struct {
	CreatedAt   time.Time
	ID          string
	Query       string
	Explanation string
	Source      CandidateSource
	Validation  ValidationReport
}

// PlannedQuery is one independently approved query from an agent workflow plan.
type PlannedQuery struct {
	ResourceType      ResourceType
	ID                string
	Query             string
	Name              string
	SchemaFingerprint string
	Groups            []string
	Completed         bool
}

// ValidationReport stores local BYDBQL validation output.
type ValidationReport struct {
	CheckedAt time.Time
	Message   string
	QueryType string
	Valid     bool
}

// Status returns a compact validation status.
func (vr ValidationReport) Status() string {
	if vr.Valid {
		return "valid"
	}
	if vr.Message == "" {
		return "not checked"
	}
	return "invalid"
}

// ExecutionResult stores read-only BYDBQL execution output.
type ExecutionResult struct {
	CheckedAt    time.Time
	Command      string
	ResourceType string
	Summary      string
	Query        string
	Path         string
	Response     string
	Error        string
	Hint         string
	Columns      []string
	Preview      [][]string
	Rows         int
	Duration     time.Duration
	Truncated    bool
}

// TranscriptEntry is one visible agent or workflow event.
type TranscriptEntry struct {
	CreatedAt time.Time
	Role      string
	Content   string
}

// ConversationTurn is one user-agent exchange in the BYDBQL drafting loop.
type ConversationTurn struct {
	CreatedAt time.Time
	Hint      string
	Response  string
	Candidate string
}

// QuerySession is the workflow contract between the TUI, agent gateway, validator, and tool executor.
type QuerySession struct {
	Schemas             map[string]SchemaSnapshot
	TimeRange           TimeRange
	AgentSessionID      string
	ResourceType        ResourceType
	UserGoal            string
	ResourceName        string
	ID                  string
	DiscoveryGoal       string
	Phase               Phase
	Validation          ValidationReport
	Transcript          []TranscriptEntry
	Groups              []string
	ChatMessages        []ChatMessage
	Conversation        []ConversationTurn
	Candidates          []BydbqlCandidate
	PlannedQueries      []PlannedQuery
	SchemaSnapshot      SchemaSnapshot
	ExecutionResult     ExecutionResult
	ActivePlanStep      int
	SelectedCandidate   int
	AutoMatched         bool
	SlotsPinned         bool
	CandidateSuperseded bool
}

// SetCatalog replaces the discovery context attached to a schema snapshot.
func (snapshot *SchemaSnapshot) SetCatalog(catalog SchemaCatalog) {
	if snapshot == nil {
		return
	}
	snapshot.AvailableGroups = append([]string(nil), catalog.Groups...)
	snapshot.Catalog = append([]CatalogEntry(nil), catalog.Entries...)
}

// SchemaKey returns a normalized identity for a resource schema.
func SchemaKey(resourceType ResourceType, name string, groups []string) string {
	normalizedGroups := make([]string, 0, len(groups))
	for _, group := range groups {
		trimmedGroup := strings.TrimSpace(group)
		if trimmedGroup != "" {
			normalizedGroups = append(normalizedGroups, trimmedGroup)
		}
	}
	sort.Strings(normalizedGroups)
	return strings.Join([]string{
		strings.ToUpper(strings.TrimSpace(resourceType.String())),
		strings.TrimSpace(name),
		strings.Join(normalizedGroups, ","),
	}, "|")
}

// CacheSchema adds or replaces a resource schema without changing the active TUI selection.
func (qs *QuerySession) CacheSchema(snapshot SchemaSnapshot) SchemaSnapshot {
	if qs == nil {
		return snapshot
	}
	preserveSchemaDiscoveryContext(&snapshot, qs.SchemaSnapshot)
	snapshot.EnsureFingerprint()
	if qs.Schemas == nil {
		qs.Schemas = make(map[string]SchemaSnapshot)
	}
	qs.Schemas[SchemaKey(snapshot.Type, snapshot.Name, snapshot.Groups)] = CloneSchemaSnapshot(snapshot)
	return snapshot
}

// ActivateSchema caches a schema and makes it the active TUI selection.
func (qs *QuerySession) ActivateSchema(snapshot SchemaSnapshot) {
	if qs == nil {
		return
	}
	snapshot = qs.CacheSchema(snapshot)
	qs.ResourceType = snapshot.Type
	qs.ResourceName = snapshot.Name
	qs.Groups = append([]string(nil), snapshot.Groups...)
	qs.SchemaSnapshot = CloneSchemaSnapshot(snapshot)
}

// CachedSchema returns the exact schema cached for a resource and group set.
func (qs *QuerySession) CachedSchema(resourceType ResourceType, name string, groups []string) (SchemaSnapshot, bool) {
	if qs == nil {
		return SchemaSnapshot{}, false
	}
	if qs.Schemas != nil {
		if snapshot, ok := qs.Schemas[SchemaKey(resourceType, name, groups)]; ok {
			return CloneSchemaSnapshot(snapshot), true
		}
	}
	activeSnapshot := qs.SchemaSnapshot
	if SchemaKey(activeSnapshot.Type, activeSnapshot.Name, activeSnapshot.Groups) != SchemaKey(resourceType, name, groups) {
		return SchemaSnapshot{}, false
	}
	activeSnapshot = qs.CacheSchema(activeSnapshot)
	return CloneSchemaSnapshot(activeSnapshot), activeSnapshot.Loaded
}

func preserveSchemaDiscoveryContext(target *SchemaSnapshot, existing SchemaSnapshot) {
	if len(target.AvailableGroups) == 0 {
		target.AvailableGroups = append([]string(nil), existing.AvailableGroups...)
	}
	if len(target.Catalog) == 0 {
		target.Catalog = append([]CatalogEntry(nil), existing.Catalog...)
	}
}

// CloneSchemaSnapshot deep-copies a snapshot so a stored schema cannot be mutated through a shared slice.
func CloneSchemaSnapshot(snapshot SchemaSnapshot) SchemaSnapshot {
	clonedSnapshot := snapshot
	clonedSnapshot.Groups = append([]string(nil), snapshot.Groups...)
	clonedSnapshot.Tags = append([]string(nil), snapshot.Tags...)
	clonedSnapshot.EntityTags = append([]string(nil), snapshot.EntityTags...)
	clonedSnapshot.Fields = append([]string(nil), snapshot.Fields...)
	clonedSnapshot.Columns = append([]SchemaColumn(nil), snapshot.Columns...)
	clonedSnapshot.IndexedFields = append([]string(nil), snapshot.IndexedFields...)
	clonedSnapshot.SortableIndexes = CloneSortableIndexes(snapshot.SortableIndexes)
	clonedSnapshot.ResourceNames = append([]string(nil), snapshot.ResourceNames...)
	clonedSnapshot.AvailableGroups = append([]string(nil), snapshot.AvailableGroups...)
	clonedSnapshot.Catalog = append([]CatalogEntry(nil), snapshot.Catalog...)
	return clonedSnapshot
}

// CloneSortableIndexes deep-copies index rules together with their tag lists.
func CloneSortableIndexes(indexes []SortableIndex) []SortableIndex {
	clonedIndexes := append([]SortableIndex(nil), indexes...)
	for indexPosition := range clonedIndexes {
		clonedIndexes[indexPosition].Tags = append([]string(nil), indexes[indexPosition].Tags...)
	}
	return clonedIndexes
}

// CurrentCandidate returns the newest candidate query.
func (qs *QuerySession) CurrentCandidate() *BydbqlCandidate {
	if qs == nil || len(qs.Candidates) == 0 {
		return nil
	}
	selectedCandidate := qs.SelectedCandidate
	if selectedCandidate < 0 || selectedCandidate >= len(qs.Candidates) {
		selectedCandidate = len(qs.Candidates) - 1
	}
	return &qs.Candidates[selectedCandidate]
}

// AddCandidate appends a candidate and updates the session validation summary.
func (qs *QuerySession) AddCandidate(candidate BydbqlCandidate) {
	qs.Candidates = append(qs.Candidates, candidate)
	qs.SelectedCandidate = len(qs.Candidates) - 1
	qs.Validation = candidate.Validation
	qs.CandidateSuperseded = false
}

// AddCandidateVersion appends one versioned query candidate and selects it.
func (qs *QuerySession) AddCandidateVersion(
	query, explanation string,
	source CandidateSource,
	validation ValidationReport,
	createdAt time.Time,
) {
	qs.AddCandidate(BydbqlCandidate{
		ID:          fmt.Sprintf("candidate-%d", len(qs.Candidates)+1),
		Query:       query,
		Explanation: explanation,
		Source:      source,
		CreatedAt:   createdAt,
		Validation:  validation,
	})
}

// SetPlannedQueries replaces the active agent workflow with compiled, exact statements.
func (qs *QuerySession) SetPlannedQueries(queries []PlannedQuery) {
	qs.PlannedQueries = append([]PlannedQuery(nil), queries...)
	qs.ActivePlanStep = 0
}

// CurrentPlannedQuery returns the next query in the compiled workflow.
func (qs *QuerySession) CurrentPlannedQuery() *PlannedQuery {
	if qs == nil || qs.ActivePlanStep < 0 || qs.ActivePlanStep >= len(qs.PlannedQueries) {
		return nil
	}
	return &qs.PlannedQueries[qs.ActivePlanStep]
}

// CompletePlannedQuery records execution and advances to the next planned statement.
func (qs *QuerySession) CompletePlannedQuery(query string) *PlannedQuery {
	currentQuery := qs.CurrentPlannedQuery()
	if currentQuery == nil || currentQuery.Query != query {
		return nil
	}
	currentQuery.Completed = true
	qs.ActivePlanStep++
	return qs.CurrentPlannedQuery()
}

// SelectCandidate makes an existing version the current candidate.
func (qs *QuerySession) SelectCandidate(index int) bool {
	if qs == nil || index < 0 || index >= len(qs.Candidates) {
		return false
	}
	qs.SelectedCandidate = index
	qs.Validation = qs.Candidates[index].Validation
	return true
}

// SelectedCandidateIndex returns the current candidate version index.
func (qs *QuerySession) SelectedCandidateIndex() int {
	if qs == nil || len(qs.Candidates) == 0 {
		return -1
	}
	if qs.SelectedCandidate < 0 || qs.SelectedCandidate >= len(qs.Candidates) {
		return len(qs.Candidates) - 1
	}
	return qs.SelectedCandidate
}

// AddConversationTurn appends one user-agent exchange to the session history.
func (qs *QuerySession) AddConversationTurn(turn ConversationTurn) {
	if strings.TrimSpace(turn.Hint) == "" && strings.TrimSpace(turn.Response) == "" && strings.TrimSpace(turn.Candidate) == "" {
		return
	}
	qs.Conversation = append(qs.Conversation, turn)
}

// AddChatMessage appends one chat entry to the visible conversation history.
func (qs *QuerySession) AddChatMessage(message ChatMessage) {
	if strings.TrimSpace(message.Content) == "" && strings.TrimSpace(message.Candidate) == "" {
		return
	}
	qs.ChatMessages = append(qs.ChatMessages, message)
}

// AddTranscript appends a visible workflow or agent event.
func (qs *QuerySession) AddTranscript(role, content string, createdAt time.Time) {
	if strings.TrimSpace(content) == "" {
		return
	}
	qs.Transcript = append(qs.Transcript, TranscriptEntry{
		Role:      role,
		Content:   content,
		CreatedAt: createdAt,
	})
}
