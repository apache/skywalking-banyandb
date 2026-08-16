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

	"github.com/charmbracelet/bubbles/textarea"
)

// maxEditorHistoryEntries bounds one editor's recall list so a long session cannot grow without limit.
const maxEditorHistoryEntries = 50

// editorHistory recalls earlier editor submissions the way a shell prompt does.
//
// The text being composed is set aside as the draft on the first recall, so stepping forward past
// the newest entry returns the user to what they were typing instead of to an empty editor.
type editorHistory struct {
	draft   string
	entries []string
	cursor  int
}

// record keeps a submission for later recall and stops browsing.
//
// An immediate repeat is collapsed, so holding the same query does not fill the list with one value.
func (history *editorHistory) record(value string) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		history.stopBrowsing()
		return
	}
	if len(history.entries) == 0 || history.entries[len(history.entries)-1] != trimmed {
		history.entries = append(history.entries, trimmed)
		if len(history.entries) > maxEditorHistoryEntries {
			history.entries = history.entries[len(history.entries)-maxEditorHistoryEntries:]
		}
	}
	history.stopBrowsing()
}

// stopBrowsing returns the cursor to the live editor without discarding recorded entries.
func (history *editorHistory) stopBrowsing() {
	history.cursor = len(history.entries)
	history.draft = ""
}

// recallPrevious steps one entry towards the oldest submission.
func (history *editorHistory) recallPrevious(current string) (string, bool) {
	if len(history.entries) == 0 || history.cursor == 0 {
		return "", false
	}
	if history.cursor >= len(history.entries) {
		history.draft = current
		history.cursor = len(history.entries)
	}
	history.cursor--
	return history.entries[history.cursor], true
}

// recallNext steps one entry towards the newest submission, then back to the draft.
func (history *editorHistory) recallNext() (string, bool) {
	if history.cursor >= len(history.entries) {
		return "", false
	}
	history.cursor++
	if history.cursor >= len(history.entries) {
		draft := history.draft
		history.draft = ""
		return draft, true
	}
	return history.entries[history.cursor], true
}

// statusLabel reports which entry is showing, counting back from the newest.
func (history editorHistory) statusLabel(subject string) string {
	if history.cursor >= len(history.entries) {
		return subject + " draft restored"
	}
	return fmt.Sprintf("%s history %d of %d", subject, history.cursor+1, len(history.entries))
}

// recallHistoryValue picks the entry an arrow key should restore, or reports that the key
// belongs to the editor.
//
// The boundary is the logical line rather than the visual row: a long query wraps across several
// rows, and counting those would cost one keypress per wrap before the history became reachable.
func recallHistoryValue(history *editorHistory, editor textarea.Model, key string) (string, bool) {
	if key == keyArrowUp {
		if editor.Line() > 0 {
			return "", false
		}
		return history.recallPrevious(editor.Value())
	}
	if editor.Line() < editor.LineCount()-1 {
		return "", false
	}
	return history.recallNext()
}
