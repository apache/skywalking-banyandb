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
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// The wrappers below all tolerate a nil session log, so a failed log setup degrades the replay file
// rather than the TUI.

func (m *Model) logWrite(category, message string) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.Write(category, message)
}

func (m *Model) logWriteError(category string, err error) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteError(category, err)
}

func (m *Model) logAgentTurn(events []agent.Event) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteAgentTurn(events)
}

func (m *Model) logQuerySession(querySession *session.QuerySession) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteQuerySession(querySession)
	m.sessionLog.WriteSchemaSnapshot("schema_snapshot", querySession.SchemaSnapshot)
	m.sessionLog.WriteChatMessages(querySession.ChatMessages)
}

// logSchemaAnswer records the schema a direct lookup put on screen, and the state that keeps it there.
func (m *Model) logSchemaAnswer() {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteSchemaSnapshot("schema_answer", m.selectedSchema)
	m.logViewState("schema answer applied")
}

// logViewState records which evidence panel owns the slot, and why.
//
// A panel that renders once and disappears leaves two of these lines with different owners, which
// names the transition that dropped it.
func (m *Model) logViewState(reason string) {
	if m.sessionLog == nil {
		return
	}
	m.sessionLog.WriteViewState(fmt.Sprintf(
		"%s · evidence=%s shows_schema=%t search_open=%t search_dismissed=%t search_value=%q focus=%s selected_schema=%s/%s loaded=%t schema_lines=%d phase=%s busy=%t",
		reason,
		m.evidenceMode,
		m.showsSchemaEvidence(),
		m.schemaSearchOpen(),
		m.schemaSearchDismissed,
		m.schemaSearchValue,
		m.focusLabel(),
		strings.Join(m.selectedSchema.Groups, "|"),
		m.selectedSchema.Name,
		m.selectedSchema.Loaded,
		len(schemaDetailLines(m.selectedSchema)),
		m.currentPhaseLabel(),
		m.busy,
	))
}
