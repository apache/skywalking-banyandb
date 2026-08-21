// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package app

import (
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/bridge"
)

// Turn progress is derived from the controlled tool calls the turn actually made, so a stage only
// appears once a tool reports it. An operation label covers the gap before the first tool call.

type turnProgressState int

const (
	turnProgressPending turnProgressState = iota
	turnProgressRunning
	turnProgressSucceeded
	turnProgressFailed
)

type turnProgressStage struct {
	label string
	state turnProgressState
}

type turnProgressStageID int

const (
	turnProgressStageCatalog turnProgressStageID = iota
	turnProgressStageDescribeSchema
	turnProgressStageCompilePlan
	turnProgressStageValidate
	turnProgressStageExecute
	turnProgressStageCount
)

type progressOperation int

const (
	progressOperationPreparing progressOperation = iota
	progressOperationCatalog
	progressOperationValidate
	progressOperationExecute
	progressOperationSchema
)

func (operation progressOperation) label() string {
	switch operation {
	case progressOperationCatalog:
		return "catalog"
	case progressOperationValidate:
		return "validate"
	case progressOperationExecute:
		return "execute"
	case progressOperationSchema:
		return "describe schema"
	default:
		return "preparing"
	}
}

func (m Model) renderTurnProgress() string {
	if !m.busy && len(m.turnEvents) == 0 {
		return ""
	}
	stages := [turnProgressStageCount]turnProgressStage{
		turnProgressStageCatalog:        {label: "catalog"},
		turnProgressStageDescribeSchema: {label: "describe schema"},
		turnProgressStageCompilePlan:    {label: "compile plan"},
		turnProgressStageValidate:       {label: "validate"},
		turnProgressStageExecute:        {label: "execute"},
	}
	observedStages := [turnProgressStageCount]bool{}
	for _, event := range m.turnEvents {
		stageIndex, ok := progressStageForEvent(event)
		if !ok {
			continue
		}
		observedStages[stageIndex] = true
		stages[stageIndex].state = progressStateForEvent(event)
	}
	parts := make([]string, 0, len(stages))
	for stageIndex, stage := range stages {
		if !observedStages[stageIndex] {
			continue
		}
		parts = append(parts, renderTurnProgressStage(stage))
	}
	if len(parts) == 0 && m.busy {
		return mutedStyle.Render("Steps  " + warnStyle.Render("⟳ "+m.progressOperation.label()))
	}
	if len(parts) == 0 {
		return ""
	}
	return mutedStyle.Render("Steps  " + strings.Join(parts, " · "))
}

func progressStageForEvent(event agent.Event) (turnProgressStageID, bool) {
	switch event.ToolName {
	case bridge.ToolListGroupsSchemas:
		return turnProgressStageCatalog, true
	case bridge.ToolDescribeSchema:
		return turnProgressStageDescribeSchema, true
	case bridge.ToolProposeQueryPlan:
		return turnProgressStageCompilePlan, true
	case bridge.ToolValidateBydbQL:
		return turnProgressStageValidate, true
	case bridge.ToolExecuteBydbQL:
		return turnProgressStageExecute, true
	default:
		return turnProgressStageCatalog, false
	}
}

func progressStateForEvent(event agent.Event) turnProgressState {
	if event.Kind == agent.EventKindToolCall || event.Status == agent.EventStatusRunning || event.Status == agent.EventStatusWaiting {
		return turnProgressRunning
	}
	if event.Status == agent.EventStatusFailed || event.Err != nil {
		return turnProgressFailed
	}
	if event.Status == agent.EventStatusSucceeded {
		return turnProgressSucceeded
	}
	return turnProgressPending
}

func renderTurnProgressStage(stage turnProgressStage) string {
	switch stage.state {
	case turnProgressRunning:
		return warnStyle.Render("⟳ " + stage.label)
	case turnProgressSucceeded:
		return okStyle.Render("✓ " + stage.label)
	case turnProgressFailed:
		return badStyle.Render("! " + stage.label)
	default:
		return mutedStyle.Render("○ " + stage.label)
	}
}
