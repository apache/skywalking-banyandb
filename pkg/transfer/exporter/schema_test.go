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

package exporter

import (
	"strings"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// groupWithStages builds a *commonv1.Group with the given stage names declared in ResourceOpts.
func groupWithStages(name string, stageNames ...string) *commonv1.Group {
	stages := make([]*commonv1.LifecycleStage, 0, len(stageNames))
	for _, s := range stageNames {
		stages = append(stages, &commonv1.LifecycleStage{Name: s})
	}
	var resOpts *commonv1.ResourceOpts
	if len(stages) > 0 {
		resOpts = &commonv1.ResourceOpts{Stages: stages}
	}
	return &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: name},
		ResourceOpts: resOpts,
	}
}

// segmentUnit constructs a minimal segment unit of the given group.
func segmentUnit(group string) *transferv1.UnitInventory {
	return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{
		Segment: &transferv1.SegmentInventory{Unit: &transferv1.SegmentUnit{Group: group}},
	}}
}

// planFrame constructs a minimal units frame with one segment unit for the given group.
func planFrame(nodeID, stage, group string) *transferv1.UnitFrame {
	return &transferv1.UnitFrame{NodeId: nodeID, Stage: stage, Units: []*transferv1.UnitInventory{segmentUnit(group)}}
}

func TestUniqueSorted_NilForEmptyInput(t *testing.T) {
	if got := uniqueSorted(nil); got != nil {
		t.Fatalf("uniqueSorted(nil) = %v, want nil", got)
	}
	if got := uniqueSorted([]string{}); got != nil {
		t.Fatalf("uniqueSorted([]) = %v, want nil", got)
	}
}

func TestUniqueSorted_DedupsAndSorts(t *testing.T) {
	got := uniqueSorted([]string{"c", "a", "b", "a", "c"})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("uniqueSorted = %v, want %v", got, want)
	}
	for i, v := range want {
		if got[i] != v {
			t.Fatalf("uniqueSorted[%d] = %q, want %q", i, got[i], v)
		}
	}
}

func TestStageWarnings_NoWarningWhenStagesDeclared(t *testing.T) {
	// Group with no declared stages: StageNames returns ["hot"].
	gHot := groupWithStages("g-hot")
	// Group with a warm stage declared: StageNames returns ["hot", "warm"].
	gWarm := groupWithStages("g-warm", "warm")

	propertyFrame := &transferv1.UnitFrame{NodeId: "node1", Stage: "hot", Units: []*transferv1.UnitInventory{
		{Kind: &transferv1.UnitInventory_Property{Property: &transferv1.PropertyInventory{Group: "g-hot"}}},
	}}
	result := &PlanResult{
		Frames: []*transferv1.UnitFrame{
			// RunPlan normalizes the default tier to "hot", which is in StageNames of both groups.
			planFrame("node1", "hot", "g-hot"),
			planFrame("node1", "hot", "g-warm"),
			// "warm" is declared for g-warm.
			planFrame("node2", "warm", "g-warm"),
			// A property unit names its group on the PropertyInventory.
			propertyFrame,
		},
	}
	warnings := StageWarnings([]*commonv1.Group{gHot, gWarm}, result)
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
}

func TestStageWarnings_WarnWhenStageNotDeclaredForGroup(t *testing.T) {
	// Group declares no stages beyond "hot" (the default).
	g := groupWithStages("g1")
	result := &PlanResult{
		Frames: []*transferv1.UnitFrame{
			planFrame("node1", "warm", "g1"),
		},
	}
	warnings := StageWarnings([]*commonv1.Group{g}, result)
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %v", warnings)
	}
	w := warnings[0]
	if !strings.Contains(w, "node1") || !strings.Contains(w, "warm") || !strings.Contains(w, "g1") {
		t.Fatalf("warning must name the node, stage and group: %q", w)
	}
}

func TestStageWarnings_WarnWhenGroupAbsentFromSchema(t *testing.T) {
	g := groupWithStages("g1")
	result := &PlanResult{
		Frames: []*transferv1.UnitFrame{
			planFrame("node1", "hot", "unknown-group"),
		},
	}
	warnings := StageWarnings([]*commonv1.Group{g}, result)
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning for absent group, got %v", warnings)
	}
	if !strings.Contains(warnings[0], "unknown-group") {
		t.Fatalf("warning must name the absent group: %q", warnings[0])
	}
}

func TestStageWarnings_IdenticalWarningsAreCollapsed(t *testing.T) {
	g := groupWithStages("g1")
	// Two units in the same frame with the same group produce identical warning text.
	frame := &transferv1.UnitFrame{NodeId: "node1", Stage: "warm", Units: []*transferv1.UnitInventory{segmentUnit("g1"), segmentUnit("g1")}}
	result := &PlanResult{Frames: []*transferv1.UnitFrame{frame}}
	warnings := StageWarnings([]*commonv1.Group{g}, result)
	if len(warnings) != 1 {
		t.Fatalf("identical warnings must be collapsed to 1, got %v", warnings)
	}
}
