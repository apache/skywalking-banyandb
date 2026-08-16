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

package app

import (
	tea "github.com/charmbracelet/bubbletea"
)

// mouseScrollStep is the row count one wheel notch moves inside a focused panel.
const mouseScrollStep = 3

// panelRegion maps the rendered row span of one panel to the focus target it owns.
type panelRegion struct {
	focus  int
	top    int
	bottom int
	left   int
	right  int
}

// contains reports whether a screen cell falls inside the region.
func (region panelRegion) contains(column, row int) bool {
	return row >= region.top && row <= region.bottom && column >= region.left && column <= region.right
}

// handleMouse routes a click to the panel under the cursor and scrolls the focused panel on wheel events.
func (m *Model) handleMouse(mouseMsg tea.MouseMsg) (tea.Cmd, bool) {
	if m.quitConfirmPending || m.helpVisible {
		return nil, false
	}
	switch mouseMsg.Action {
	case tea.MouseActionPress:
		switch mouseMsg.Button {
		case tea.MouseButtonLeft:
			return m.focusPanelAt(mouseMsg.X, mouseMsg.Y)
		case tea.MouseButtonWheelUp:
			m.scrollFocusedPanel(-mouseScrollStep)
			return nil, true
		case tea.MouseButtonWheelDown:
			m.scrollFocusedPanel(mouseScrollStep)
			return nil, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

// focusPanelAt moves focus to whichever rendered panel contains the clicked cell.
//
// Regions are recomputed here rather than after every keystroke, since only a click needs them.
func (m *Model) focusPanelAt(column, row int) (tea.Cmd, bool) {
	if m.regionsStale {
		m.refreshPanelRegions()
	}
	for _, region := range m.panelRegions {
		if !region.contains(column, row) {
			continue
		}
		if region.focus == m.focus {
			return nil, true
		}
		m.focus = region.focus
		if region.focus == focusExecution {
			m.focusEvidencePanel()
		}
		m.status = m.focusLabel() + " focused"
		return m.syncFocus(), true
	}
	return nil, false
}

// scrollFocusedPanel applies wheel movement to the list or detail owned by the focused panel.
func (m *Model) scrollFocusedPanel(delta int) {
	switch m.focus {
	case focusChat:
		m.moveChatCursor(delta, m.chatListViewportHeight())
	case focusExecution:
		if m.evidenceMode.showsSchema() {
			m.scrollSchemaDetail(delta)
			return
		}
		m.moveExecutionRowCursor(delta)
	default:
		if m.evidenceMode.showsSchema() {
			m.scrollSchemaDetail(delta)
		}
	}
}
