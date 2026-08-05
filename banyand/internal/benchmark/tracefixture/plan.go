// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Package tracefixture builds the deterministic hybrid trace merge benchmark fixture.
package tracefixture

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	// MatureTraceCount is the number of complete mature source traces.
	MatureTraceCount = 31_832
	// SmallTraceCount is the number of structurally closed small-write traces.
	SmallTraceCount = 254
	// CopyTraceCount is the number of complete mature-template copies.
	CopyTraceCount = 5_202
	// GeneratedTraceCount is the fixture's total logical trace count.
	GeneratedTraceCount = 37_288
	// ObservedRows is the total row count in one cycle of observed small writes.
	ObservedRows = 1_214
	// ObservedWrites is the number of writes in one observed shape cycle.
	ObservedWrites = 12
)

// TraceClass identifies how a generated trace entered the fixture.
type TraceClass string

const (
	// TraceClassMature is an unchanged complete mature source trace.
	TraceClassMature TraceClass = "mature_base"
	// TraceClassSmall is a structurally closed trace from the observed small writes.
	TraceClassSmall TraceClass = "small_closed"
	// TraceClassCopy is a complete copy of an independently selected mature template.
	TraceClassCopy TraceClass = "mature_copy"
)

// Shape is one observed physical write target.
type Shape struct {
	PartID string `json:"partID"`
	Blocks uint64 `json:"blocks"`
	Rows   uint64 `json:"rows"`
}

// Fragment is an indivisible source block belonging to a complete logical trace.
type Fragment struct {
	SourcePartID uint64 `json:"sourcePartID"`
	MinTimestamp int64  `json:"minTimestamp"`
	MaxTimestamp int64  `json:"maxTimestamp"`
	Rows         uint64 `json:"rows"`
}

// Trace is one complete source trace before deterministic ID remapping.
type Trace struct {
	SourceID  string
	Fragments []Fragment
}

// Instance is one generated complete logical trace.
type Instance struct {
	SourceID      string     `json:"sourceTraceID"`
	GeneratedID   string     `json:"generatedTraceID"`
	Class         TraceClass `json:"class"`
	Fragments     []Fragment `json:"-"`
	CopyOrdinal   int        `json:"copyOrdinal"`
	StreamOrdinal int        `json:"streamOrdinal"`
}

// ScheduledFragment records one whole fragment assigned to a physical write.
type ScheduledFragment struct {
	GeneratedTraceID string `json:"generatedTraceID"`
	SourceTraceID    string `json:"sourceTraceID"`
	SourcePartID     uint64 `json:"sourcePartID"`
	Rows             uint64 `json:"rows"`
	FragmentOrdinal  int    `json:"fragmentOrdinal"`
	InstanceOrdinal  int    `json:"instanceOrdinal"`
}

// Write schedules one production write in the accelerated logical day.
type Write struct {
	Publication              time.Time           `json:"publication"`
	IndexSHA256              map[string]string   `json:"indexSHA256"`
	IndexCompressedBytes     map[string]uint64   `json:"indexCompressedBytes"`
	PartID                   string              `json:"partID"`
	CoreSHA256               string              `json:"coreSHA256"`
	BoundaryChoice           string              `json:"boundaryChoice"`
	Fragments                []ScheduledFragment `json:"fragments"`
	Shape                    Shape               `json:"shape"`
	TargetCumulativeBlocks   uint64              `json:"targetCumulativeBlocks"`
	AfterDistance            float64             `json:"afterDistance"`
	BeforeDistance           float64             `json:"beforeDistance"`
	TargetCumulativeRows     uint64              `json:"targetCumulativeRows"`
	RealizedCumulativeBlocks uint64              `json:"realizedCumulativeBlocks"`
	RealizedCumulativeRows   uint64              `json:"realizedCumulativeRows"`
	TemplateOrdinal          int                 `json:"templateOrdinal"`
	MinTimestamp             int64               `json:"minTimestamp"`
	MaxTimestamp             int64               `json:"maxTimestamp"`
	CoreCompressedBytes      uint64              `json:"coreCompressedBytes"`
	Rows                     uint64              `json:"rows"`
	Blocks                   uint64              `json:"blocks"`
	PartialTail              bool                `json:"partialTail"`
}

// Plan is the deterministic generated-ID and physical-write schedule.
type Plan struct {
	DayStart       time.Time     `json:"dayStart"`
	Instances      []Instance    `json:"instances"`
	Writes         []Write       `json:"writes"`
	DayDuration    time.Duration `json:"dayDuration"`
	WriteIntensity int           `json:"writeIntensity"`
}

// Options configures deterministic fixture scheduling.
type Options struct {
	DayStart       time.Time
	Shapes         []Shape
	DayDuration    time.Duration
	CopyCount      int
	WriteIntensity int
}

type fragmentRef struct {
	fragment        Fragment
	instanceOrdinal int
	fragmentOrdinal int
}

// DefaultShapes returns the chronological cycle measured from the downloaded shard.
func DefaultShapes() []Shape {
	return []Shape{
		{PartID: "21f1", Blocks: 45, Rows: 262},
		{PartID: "2223", Blocks: 46, Rows: 117},
		{PartID: "222c", Blocks: 13, Rows: 31},
		{PartID: "2233", Blocks: 9, Rows: 36},
		{PartID: "223a", Blocks: 49, Rows: 188},
		{PartID: "2248", Blocks: 32, Rows: 180},
		{PartID: "2256", Blocks: 26, Rows: 164},
		{PartID: "2259", Blocks: 7, Rows: 33},
		{PartID: "2264", Blocks: 32, Rows: 137},
		{PartID: "2265", Blocks: 6, Rows: 28},
		{PartID: "2266", Blocks: 4, Rows: 14},
		{PartID: "226b", Blocks: 3, Rows: 24},
	}
}

// BuildPlan expands complete source traces, remaps their IDs, and schedules whole fragments.
func BuildPlan(mature, small []Trace, options Options) (Plan, error) {
	if options.DayDuration <= 0 {
		return Plan{}, fmt.Errorf("day duration must be positive")
	}
	if len(options.Shapes) == 0 {
		return Plan{}, fmt.Errorf("at least one write shape is required")
	}
	if options.CopyCount < 0 || options.CopyCount > len(mature) {
		return Plan{}, fmt.Errorf("copy count %d is outside mature population [0,%d]", options.CopyCount, len(mature))
	}
	if options.WriteIntensity < 0 {
		return Plan{}, fmt.Errorf("write intensity must be positive")
	}
	if options.WriteIntensity == 0 {
		options.WriteIntensity = 1
	}
	if sourceErr := validateSourceTraces(mature, small); sourceErr != nil {
		return Plan{}, sourceErr
	}
	instances := expandWriteStreams(buildInstances(mature, small, options.CopyCount), options.WriteIntensity)
	generatedIDs := make(map[string]struct{}, len(instances))
	for instanceIdx := range instances {
		generatedID := instances[instanceIdx].GeneratedID
		if _, ok := generatedIDs[generatedID]; ok {
			return Plan{}, fmt.Errorf("generated trace ID collision: %q", generatedID)
		}
		generatedIDs[generatedID] = struct{}{}
	}
	refs := flattenFragments(instances)
	totalRows := sumFragmentRows(refs)
	writesCount := int((totalRows*ObservedWrites + ObservedRows - 1) / ObservedRows)
	if writesCount > len(refs) {
		return Plan{}, fmt.Errorf("cannot assign %d writes from only %d whole fragments", writesCount, len(refs))
	}
	writes, scheduleErr := scheduleWrites(instances, refs, writesCount, options)
	if scheduleErr != nil {
		return Plan{}, scheduleErr
	}
	return Plan{DayStart: options.DayStart, DayDuration: options.DayDuration, WriteIntensity: options.WriteIntensity, Instances: instances, Writes: writes}, nil
}

func validateSourceTraces(populations ...[]Trace) error {
	seen := make(map[string]struct{})
	for _, population := range populations {
		for traceIdx := range population {
			trace := &population[traceIdx]
			if trace.SourceID == "" {
				return fmt.Errorf("source trace at index %d has an empty ID", traceIdx)
			}
			if len(trace.Fragments) == 0 {
				return fmt.Errorf("source trace %q has no fragments", trace.SourceID)
			}
			if _, ok := seen[trace.SourceID]; ok {
				return fmt.Errorf("source trace ID %q appears in multiple populations", trace.SourceID)
			}
			seen[trace.SourceID] = struct{}{}
			for fragmentIdx := range trace.Fragments {
				fragment := &trace.Fragments[fragmentIdx]
				if fragment.Rows == 0 {
					return fmt.Errorf("source trace %q fragment %d has no rows", trace.SourceID, fragmentIdx)
				}
				if fragment.MaxTimestamp < fragment.MinTimestamp {
					return fmt.Errorf("source trace %q fragment %d has an inverted timestamp range", trace.SourceID, fragmentIdx)
				}
			}
		}
	}
	return nil
}

func buildInstances(mature, small []Trace, copyCount int) []Instance {
	instances := make([]Instance, 0, len(mature)+len(small)+copyCount)
	appendPopulation := func(population []Trace, class TraceClass) {
		for traceIdx := range population {
			trace := &population[traceIdx]
			instances = append(instances, newInstance(*trace, class, 0))
		}
	}
	appendPopulation(mature, TraceClassMature)
	appendPopulation(small, TraceClassSmall)
	for copyIdx, trace := range selectCopyTemplates(mature, copyCount) {
		instances = append(instances, newInstance(trace, TraceClassCopy, copyIdx+1))
	}
	sort.Slice(instances, func(leftIdx, rightIdx int) bool {
		return instances[leftIdx].GeneratedID < instances[rightIdx].GeneratedID
	})
	return instances
}

func expandWriteStreams(base []Instance, intensity int) []Instance {
	instances := make([]Instance, 0, len(base)*intensity)
	for streamOrdinal := 0; streamOrdinal < intensity; streamOrdinal++ {
		for baseIdx := range base {
			instance := base[baseIdx]
			instance.Fragments = append([]Fragment(nil), instance.Fragments...)
			instance.StreamOrdinal = streamOrdinal
			if streamOrdinal > 0 {
				instance.GeneratedID = generatedStreamID(instance.Class, instance.SourceID, instance.CopyOrdinal, streamOrdinal)
			}
			instances = append(instances, instance)
		}
	}
	sort.Slice(instances, func(leftIdx, rightIdx int) bool {
		return instances[leftIdx].GeneratedID < instances[rightIdx].GeneratedID
	})
	return instances
}

func newInstance(trace Trace, class TraceClass, ordinal int) Instance {
	fragments := append([]Fragment(nil), trace.Fragments...)
	mappedID := generatedID(class, trace.SourceID, ordinal)
	return Instance{SourceID: trace.SourceID, GeneratedID: mappedID, Class: class, CopyOrdinal: ordinal, Fragments: fragments}
}

func selectCopyTemplates(mature []Trace, count int) []Trace {
	selected := append([]Trace(nil), mature...)
	sort.Slice(selected, func(leftIdx, rightIdx int) bool {
		leftHash := sha256.Sum256([]byte(selected[leftIdx].SourceID))
		rightHash := sha256.Sum256([]byte(selected[rightIdx].SourceID))
		if leftHash != rightHash {
			return string(leftHash[:]) < string(rightHash[:])
		}
		return selected[leftIdx].SourceID < selected[rightIdx].SourceID
	})
	return selected[:count]
}

func generatedID(class TraceClass, sourceID string, ordinal int) string {
	family := sourceID
	if separator := strings.IndexByte(sourceID, '.'); separator > 0 {
		family = sourceID[:separator]
	}
	familyDigest := sha256.Sum256([]byte("trace-merge-fixture-family-v1\x00" + family))
	instanceDigest := sha256.Sum256([]byte(fmt.Sprintf("trace-merge-fixture-instance-v1\x00%s\x00%s\x00%d", class, sourceID, ordinal)))
	var mapped [16]byte
	copy(mapped[:6], familyDigest[:6])
	copy(mapped[6:], instanceDigest[:10])
	hexDigest := hex.EncodeToString(mapped[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexDigest[:8], hexDigest[8:12], hexDigest[12:16], hexDigest[16:20], hexDigest[20:32])
}

func generatedStreamID(class TraceClass, sourceID string, copyOrdinal, streamOrdinal int) string {
	family := sourceID
	if separator := strings.IndexByte(sourceID, '.'); separator > 0 {
		family = sourceID[:separator]
	}
	familyDigest := sha256.Sum256([]byte("trace-merge-fixture-family-v1\x00" + family))
	instanceDigest := sha256.Sum256([]byte(fmt.Sprintf("trace-merge-fixture-stream-v1\x00%s\x00%s\x00%d\x00%d",
		class, sourceID, copyOrdinal, streamOrdinal)))
	var mapped [16]byte
	copy(mapped[:6], familyDigest[:6])
	copy(mapped[6:], instanceDigest[:10])
	hexDigest := hex.EncodeToString(mapped[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexDigest[:8], hexDigest[8:12], hexDigest[12:16], hexDigest[16:20], hexDigest[20:32])
}

func flattenFragments(instances []Instance) []fragmentRef {
	refs := make([]fragmentRef, 0)
	for instanceIdx := range instances {
		instance := &instances[instanceIdx]
		for fragmentIdx := range instance.Fragments {
			refs = append(refs, fragmentRef{fragment: instance.Fragments[fragmentIdx], instanceOrdinal: instanceIdx, fragmentOrdinal: fragmentIdx})
		}
	}
	sort.SliceStable(refs, func(leftIdx, rightIdx int) bool {
		left := &refs[leftIdx]
		right := &refs[rightIdx]
		if left.fragment.MinTimestamp != right.fragment.MinTimestamp {
			return left.fragment.MinTimestamp < right.fragment.MinTimestamp
		}
		leftID := instances[left.instanceOrdinal].GeneratedID
		rightID := instances[right.instanceOrdinal].GeneratedID
		if leftID != rightID {
			return leftID < rightID
		}
		return left.fragmentOrdinal < right.fragmentOrdinal
	})
	return refs
}

func sumFragmentRows(refs []fragmentRef) uint64 {
	var total uint64
	for refIdx := range refs {
		total += refs[refIdx].fragment.Rows
	}
	return total
}

func scheduleWrites(instances []Instance, refs []fragmentRef, writesCount int, options Options) ([]Write, error) {
	writes := make([]Write, 0, writesCount)
	remaining := append([]fragmentRef(nil), refs...)
	var cumulativeBlocks, cumulativeRows uint64
	for writeIdx := 0; writeIdx < writesCount; writeIdx++ {
		shape := options.Shapes[writeIdx%len(options.Shapes)]
		remainingWrites := writesCount - writeIdx - 1
		limit := len(remaining) - remainingWrites
		if limit < 1 {
			return nil, fmt.Errorf("write %d cannot retain one fragment for each remaining write", writeIdx)
		}
		targetBlocks, targetRows := cumulativeTargets(options.Shapes, writeIdx+1)
		writeRefs, selectedIndexes, selectErr := selectFeasibleFragments(remaining, limit, remainingWrites, cumulativeBlocks, cumulativeRows,
			targetBlocks, targetRows, shape)
		if selectErr != nil {
			return nil, fmt.Errorf("cannot select chronological fragments for write %d: %w", writeIdx, selectErr)
		}
		remaining = removeSelectedFragments(remaining, selectedIndexes)
		write := Write{
			Publication: publicationAt(options.DayStart, options.DayDuration, writesCount, writeIdx), Shape: shape,
			TargetCumulativeBlocks: targetBlocks, TargetCumulativeRows: targetRows, TemplateOrdinal: writeIdx % len(options.Shapes),
		}
		for refIdx := range writeRefs {
			ref := &writeRefs[refIdx]
			instance := &instances[ref.instanceOrdinal]
			write.Fragments = append(write.Fragments, ScheduledFragment{
				GeneratedTraceID: instance.GeneratedID,
				SourceTraceID:    instance.SourceID,
				SourcePartID:     ref.fragment.SourcePartID,
				Rows:             ref.fragment.Rows,
				FragmentOrdinal:  ref.fragmentOrdinal,
				InstanceOrdinal:  ref.instanceOrdinal,
			})
			write.Blocks++
			write.Rows += ref.fragment.Rows
		}
		cumulativeBlocks += write.Blocks
		cumulativeRows += write.Rows
		write.RealizedCumulativeBlocks = cumulativeBlocks
		write.RealizedCumulativeRows = cumulativeRows
		write.AfterDistance = normalizedDistance(cumulativeBlocks, targetBlocks, shape.Blocks) + normalizedDistance(cumulativeRows, targetRows, shape.Rows)
		write.BeforeDistance = write.AfterDistance
		if len(writeRefs) > 1 {
			beforeBlocks := cumulativeBlocks - 1
			beforeRows := cumulativeRows - writeRefs[len(writeRefs)-1].fragment.Rows
			write.BeforeDistance = normalizedDistance(beforeBlocks, targetBlocks, shape.Blocks) + normalizedDistance(beforeRows, targetRows, shape.Rows)
		}
		write.BoundaryChoice = "after"
		if write.BeforeDistance < write.AfterDistance {
			write.BoundaryChoice = "after_feasibility"
		}
		writes = append(writes, write)
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("scheduler left %d fragments unassigned", len(remaining))
	}
	if len(writes) > 0 {
		last := &writes[len(writes)-1]
		last.PartialTail = last.Blocks != last.Shape.Blocks || last.Rows != last.Shape.Rows
	}
	return writes, nil
}

func selectFeasibleFragments(refs []fragmentRef, limit, remainingWrites int, currentBlocks, currentRows, targetBlocks, targetRows uint64,
	shape Shape,
) ([]fragmentRef, []int, error) {
	desiredCount := chooseCut(refs, limit, currentBlocks, currentRows, targetBlocks, targetRows, shape)
	firstSegmentSize, minimumSegments := chronologicalSegmentShape(refs)
	availableWrites := remainingWrites + 1
	if minimumSegments > availableWrites {
		return nil, nil, fmt.Errorf("%d trace-separating segments remain for only %d writes", minimumSegments, availableWrites)
	}
	targetCount := min(desiredCount, firstSegmentSize, limit)
	if minimumSegments == availableWrites {
		targetCount = firstSegmentSize
	}
	if targetCount > limit {
		return nil, nil, fmt.Errorf("required chronological segment has %d fragments but limit is %d", targetCount, limit)
	}
	selectedIndexes := make([]int, targetCount)
	for selectedIdx := range selectedIndexes {
		selectedIndexes[selectedIdx] = selectedIdx
	}
	return append([]fragmentRef(nil), refs[:targetCount]...), selectedIndexes, nil
}

func chronologicalSegmentShape(refs []fragmentRef) (int, int) {
	firstSegmentSize := len(refs)
	segments := 1
	seen := make(map[int]struct{})
	for refIdx := range refs {
		instanceOrdinal := refs[refIdx].instanceOrdinal
		if _, duplicate := seen[instanceOrdinal]; duplicate {
			if firstSegmentSize == len(refs) {
				firstSegmentSize = refIdx
			}
			segments++
			clear(seen)
		}
		seen[instanceOrdinal] = struct{}{}
	}
	return firstSegmentSize, segments
}

func removeSelectedFragments(refs []fragmentRef, selectedIndexes []int) []fragmentRef {
	selected := make(map[int]struct{}, len(selectedIndexes))
	for _, selectedIdx := range selectedIndexes {
		selected[selectedIdx] = struct{}{}
	}
	remaining := make([]fragmentRef, 0, len(refs)-len(selectedIndexes))
	for refIdx := range refs {
		if _, ok := selected[refIdx]; !ok {
			remaining = append(remaining, refs[refIdx])
		}
	}
	return remaining
}

func cumulativeTargets(shapes []Shape, completedWrites int) (uint64, uint64) {
	var cycleBlocks, cycleRows uint64
	for shapeIdx := range shapes {
		cycleBlocks += shapes[shapeIdx].Blocks
		cycleRows += shapes[shapeIdx].Rows
	}
	cycles := uint64(completedWrites / len(shapes))
	blocks := cycles * cycleBlocks
	rows := cycles * cycleRows
	for shapeIdx := 0; shapeIdx < completedWrites%len(shapes); shapeIdx++ {
		blocks += shapes[shapeIdx].Blocks
		rows += shapes[shapeIdx].Rows
	}
	return blocks, rows
}

func chooseCut(refs []fragmentRef, limit int, currentBlocks, currentRows, targetBlocks, targetRows uint64, shape Shape) int {
	bestCut := 1
	bestScore := math.Inf(1)
	var rows uint64
	for cut := 1; cut <= limit; cut++ {
		rows += refs[cut-1].fragment.Rows
		blocksAfter := currentBlocks + uint64(cut)
		rowsAfter := currentRows + rows
		score := normalizedDistance(blocksAfter, targetBlocks, shape.Blocks) + normalizedDistance(rowsAfter, targetRows, shape.Rows)
		if score < bestScore {
			bestScore = score
			bestCut = cut
		}
	}
	return bestCut
}

func normalizedDistance(actual, target, scale uint64) float64 {
	if scale == 0 {
		scale = 1
	}
	delta := float64(actual) - float64(target)
	return math.Abs(delta) / float64(scale)
}

func publicationAt(start time.Time, duration time.Duration, writesCount, writeIdx int) time.Time {
	offset := time.Duration((int64(duration) * int64(writeIdx)) / int64(writesCount))
	return start.Add(offset)
}
