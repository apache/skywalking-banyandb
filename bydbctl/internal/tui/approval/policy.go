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

package approval

import "strings"

// ExecutionPolicy controls how BYDBQL execution approvals are granted.
type ExecutionPolicy string

// Execution policies.
const (
	PolicyAskEveryTime ExecutionPolicy = "ask_every_time"
	PolicyAutoProbe    ExecutionPolicy = "auto_probe"
	PolicyTrustSession ExecutionPolicy = "trust_session"
)

// NormalizeExecutionPolicy returns a supported execution policy.
func NormalizeExecutionPolicy(value string) ExecutionPolicy {
	switch ExecutionPolicy(strings.TrimSpace(value)) {
	case PolicyAutoProbe:
		return PolicyAutoProbe
	case PolicyTrustSession:
		return PolicyTrustSession
	default:
		return PolicyAskEveryTime
	}
}

// Label returns a short UI label for the policy.
func (policy ExecutionPolicy) Label() string {
	switch policy {
	case PolicyAutoProbe:
		return "auto probe"
	case PolicyTrustSession:
		return "trust session"
	default:
		return "ask every time"
	}
}

// Next cycles to the next policy in the UI rotation order.
func (policy ExecutionPolicy) Next() ExecutionPolicy {
	switch policy {
	case PolicyAskEveryTime:
		return PolicyAutoProbe
	case PolicyAutoProbe:
		return PolicyTrustSession
	default:
		return PolicyAskEveryTime
	}
}

// AutoApprove reports whether a request should bypass interactive approval.
func (policy ExecutionPolicy) AutoApprove(source Source, probe bool, query string) bool {
	if !IsReadOnlyBYDBQL(query) {
		return false
	}
	switch policy {
	case PolicyTrustSession:
		return true
	case PolicyAutoProbe:
		return source == SourceAgentProbe && probe
	default:
		return false
	}
}
