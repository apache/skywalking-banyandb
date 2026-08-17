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

package agent

import (
	"encoding/json"
	"strings"
)

const maxProviderExecutionErrorRunes = 320

// SanitizeExecutionErrorForProvider returns a provider-safe execution error message.
func SanitizeExecutionErrorForProvider(executionError string) string {
	trimmedError := strings.TrimSpace(executionError)
	if trimmedError == "" {
		return ""
	}
	if message := extractJSONErrorMessage(trimmedError); message != "" {
		return truncateProviderError(message)
	}
	if isTransportExecutionError(trimmedError) {
		return "BYDBQL execution failed: transport error"
	}
	return truncateProviderError(trimmedError)
}

func extractJSONErrorMessage(executionError string) string {
	startIdx := strings.Index(executionError, "{")
	if startIdx < 0 {
		return ""
	}
	var payload struct {
		Message string `json:"message"`
	}
	if unmarshalErr := json.Unmarshal([]byte(executionError[startIdx:]), &payload); unmarshalErr != nil {
		return ""
	}
	return strings.TrimSpace(payload.Message)
}

func isTransportExecutionError(executionError string) bool {
	lowerError := strings.ToLower(executionError)
	transportMarkers := []string{
		"://",
		"dial tcp",
		"connect: connection refused",
		"no such host",
		"i/o timeout",
		"tls:",
		"certificate",
	}
	for _, marker := range transportMarkers {
		if strings.Contains(lowerError, marker) {
			return true
		}
	}
	return false
}

func truncateProviderError(executionError string) string {
	runes := []rune(executionError)
	if len(runes) <= maxProviderExecutionErrorRunes {
		return executionError
	}
	return string(runes[:maxProviderExecutionErrorRunes-1]) + "…"
}

// SameToolSet reports whether two tool-name lists hold the same names, ignoring order.
//
// Gateways compare an MCP server's advertised tools against the controlled set, so a reordered list
// must not read as a different one.
func SameToolSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := make(map[string]struct{}, len(left))
	for _, value := range left {
		leftSet[value] = struct{}{}
	}
	for _, value := range right {
		if _, exists := leftSet[value]; !exists {
			return false
		}
	}
	return len(leftSet) == len(left)
}

// ErrorEvent wraps an error as a terminal provider event, so every gateway reports failures alike.
func ErrorEvent(eventErr error) Event {
	return Event{
		Kind:    EventKindError,
		Message: eventErr.Error(),
		Origin:  EventOriginProvider,
		Err:     eventErr,
	}
}
