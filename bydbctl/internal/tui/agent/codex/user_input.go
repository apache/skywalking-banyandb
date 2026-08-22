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

package codex

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/agent"
)

// Codex asks the host to prompt the user through item/tool/requestUserInput. bydbctl owns all user
// interaction, so these requests are never forwarded: a controlled-tool approval is answered
// automatically, and anything else becomes a clarification event in the TUI.

const (
	maxClarificationRunes = 320
	maxUserInputQuestions = 8
	maxUserInputOptions   = 4

	normalizedControlledMCPApprovalHeader       = "approveapptoolcall?"
	normalizedControlledMCPApprovalPromptPrefix = "allowthe" + controlledMCPServerName + "mcpservertoruntool\""
	normalizedControlledMCPApprovalPromptSuffix = "\"?"
	normalizedAllowSessionOption                = "allowforthissession"
)

type toolRequestUserInputParams struct {
	ThreadID  string                         `json:"threadId"`
	TurnID    string                         `json:"turnId"`
	Questions []toolRequestUserInputQuestion `json:"questions"`
}

type toolRequestUserInputQuestion struct {
	Header   string                       `json:"header"`
	ID       string                       `json:"id"`
	Question string                       `json:"question"`
	Options  []toolRequestUserInputOption `json:"options"`
	IsOther  bool                         `json:"isOther"`
	IsSecret bool                         `json:"isSecret"`
}

type toolRequestUserInputOption struct {
	Description string `json:"description"`
	Label       string `json:"label"`
}

type toolRequestUserInputResponse struct {
	Answers map[string]toolRequestUserInputAnswer `json:"answers"`
}

type toolRequestUserInputAnswer struct {
	Answers []string `json:"answers"`
}

func (appConnection *connection) respondToUserInputRequest(id, params json.RawMessage) bool {
	var request toolRequestUserInputParams
	if unmarshalErr := json.Unmarshal(params, &request); unmarshalErr != nil || !request.valid() {
		return false
	}
	turn := appConnection.activeTurn(request.ThreadID, request.TurnID)
	if turn == nil {
		return false
	}
	if setErr := turn.setID(request.TurnID); setErr != nil {
		return false
	}
	answers, autoApproved := request.responseAnswers()
	if answers == nil {
		return false
	}
	responseBytes, marshalErr := json.Marshal(map[string]any{
		"id":     id,
		"result": toolRequestUserInputResponse{Answers: answers},
	})
	if marshalErr != nil {
		return false
	}
	if writeErr := appConnection.writeLine(responseBytes); writeErr != nil {
		return false
	}
	if autoApproved {
		return true
	}
	turn.emit(agent.Event{
		Kind:    agent.EventKindClarification,
		Message: request.clarificationMessage(),
		Origin:  agent.EventOriginProvider,
	})
	return true
}

func (request toolRequestUserInputParams) responseAnswers() (map[string]toolRequestUserInputAnswer, bool) {
	if len(request.Questions) == 1 {
		question := request.Questions[0]
		if sessionOption := question.allowControlledMCPToolForSession(); sessionOption != "" {
			return map[string]toolRequestUserInputAnswer{
				question.ID: {Answers: []string{sessionOption}},
			}, true
		}
	}
	answers := make(map[string]toolRequestUserInputAnswer, len(request.Questions))
	for _, question := range request.Questions {
		if _, exists := answers[question.ID]; exists {
			return nil, false
		}
		answers[question.ID] = toolRequestUserInputAnswer{Answers: []string{}}
	}
	return answers, false
}

func (request toolRequestUserInputParams) valid() bool {
	if strings.TrimSpace(request.ThreadID) == "" || strings.TrimSpace(request.TurnID) == "" {
		return false
	}
	if len(request.Questions) == 0 || len(request.Questions) > maxUserInputQuestions {
		return false
	}
	for _, question := range request.Questions {
		if strings.TrimSpace(question.ID) == "" || strings.TrimSpace(question.Question) == "" {
			return false
		}
	}
	return true
}

func (request toolRequestUserInputParams) clarificationMessage() string {
	const secretInputMessage = "Codex requested secret input, which bydbctl declined. Provide only non-secret query details in the composer."
	var lines []string
	for _, question := range request.Questions {
		if question.IsSecret {
			return secretInputMessage
		}
		if header := compactUserInputText(question.Header); header != "" {
			lines = append(lines, header)
		}
		lines = append(lines, compactUserInputText(question.Question))
		if options := question.optionLabels(); len(options) > 0 {
			lines = append(lines, "Options: "+strings.Join(options, " | "))
		}
		if question.IsOther {
			lines = append(lines, "You can provide another answer in the composer.")
		}
	}
	return truncateClarification(strings.Join(lines, "\n"))
}

func (question toolRequestUserInputQuestion) allowControlledMCPToolForSession() string {
	header := normalizedUserInputText(question.Header)
	prompt := normalizedUserInputText(question.Question)
	if question.IsSecret || header != normalizedControlledMCPApprovalHeader ||
		!strings.HasPrefix(prompt, normalizedControlledMCPApprovalPromptPrefix) || !strings.HasSuffix(prompt, normalizedControlledMCPApprovalPromptSuffix) {
		return ""
	}
	toolName := strings.TrimSuffix(strings.TrimPrefix(prompt, normalizedControlledMCPApprovalPromptPrefix), normalizedControlledMCPApprovalPromptSuffix)
	if !slices.Contains(controlledToolNames, toolName) {
		return ""
	}
	for _, option := range question.Options {
		if normalizedUserInputText(option.Label) == normalizedAllowSessionOption {
			return option.Label
		}
	}
	return ""
}

func (question toolRequestUserInputQuestion) optionLabels() []string {
	optionCount := len(question.Options)
	if optionCount > maxUserInputOptions {
		optionCount = maxUserInputOptions
	}
	labels := make([]string, 0, optionCount)
	for optionIdx := 0; optionIdx < optionCount; optionIdx++ {
		label := compactUserInputText(question.Options[optionIdx].Label)
		if label != "" {
			labels = append(labels, label)
		}
	}
	if len(question.Options) > maxUserInputOptions {
		labels = append(labels, "…")
	}
	return labels
}

func compactUserInputText(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func normalizedUserInputText(text string) string {
	return strings.ToLower(strings.ReplaceAll(compactUserInputText(text), " ", ""))
}

func truncateClarification(message string) string {
	runes := []rune(message)
	if len(runes) > maxClarificationRunes {
		return string(runes[:maxClarificationRunes-1]) + "…"
	}
	return message
}
