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

package bridge

import (
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// The TUI reads a session snapshot while a tool call may still be mutating the live one, so a
// snapshot is deep-copied down to every slice a caller could observe.

func cloneQuerySession(querySession *session.QuerySession) *session.QuerySession {
	if querySession == nil {
		return nil
	}
	clonedSession := *querySession
	clonedSession.Groups = append([]string(nil), querySession.Groups...)
	clonedSession.SchemaSnapshot = session.CloneSchemaSnapshot(querySession.SchemaSnapshot)
	clonedSession.Schemas = cloneSchemaStore(querySession.Schemas)
	clonedSession.Conversation = append([]session.ConversationTurn(nil), querySession.Conversation...)
	clonedSession.Candidates = cloneCandidates(querySession.Candidates)
	clonedSession.PlannedQueries = clonePlannedQueries(querySession.PlannedQueries)
	clonedSession.ExecutionResult = cloneExecutionResult(querySession.ExecutionResult)
	clonedSession.Transcript = append([]session.TranscriptEntry(nil), querySession.Transcript...)
	clonedSession.ChatMessages = cloneChatMessages(querySession.ChatMessages)
	return &clonedSession
}

func cloneSchemaStore(schemaStore map[string]session.SchemaSnapshot) map[string]session.SchemaSnapshot {
	if len(schemaStore) == 0 {
		return nil
	}
	clonedStore := make(map[string]session.SchemaSnapshot, len(schemaStore))
	for schemaKey, schemaSnapshot := range schemaStore {
		clonedStore[schemaKey] = session.CloneSchemaSnapshot(schemaSnapshot)
	}
	return clonedStore
}

func cloneCandidates(candidates []session.BydbqlCandidate) []session.BydbqlCandidate {
	return append([]session.BydbqlCandidate(nil), candidates...)
}

func clonePlannedQueries(queries []session.PlannedQuery) []session.PlannedQuery {
	clonedQueries := append([]session.PlannedQuery(nil), queries...)
	for queryIdx := range clonedQueries {
		clonedQueries[queryIdx].Groups = append([]string(nil), queries[queryIdx].Groups...)
	}
	return clonedQueries
}

func cloneExecutionResult(executionResult session.ExecutionResult) session.ExecutionResult {
	clonedResult := executionResult
	clonedResult.Columns = append([]string(nil), executionResult.Columns...)
	clonedResult.Preview = clonePreview(executionResult.Preview)
	return clonedResult
}

func cloneChatMessages(messages []session.ChatMessage) []session.ChatMessage {
	clonedMessages := append([]session.ChatMessage(nil), messages...)
	for messageIdx := range clonedMessages {
		if messages[messageIdx].Validation == nil {
			continue
		}
		clonedValidation := *messages[messageIdx].Validation
		clonedMessages[messageIdx].Validation = &clonedValidation
	}
	return clonedMessages
}

func clonePreview(preview [][]string) [][]string {
	clonedPreview := make([][]string, 0, len(preview))
	for _, row := range preview {
		clonedPreview = append(clonedPreview, append([]string(nil), row...))
	}
	return clonedPreview
}
