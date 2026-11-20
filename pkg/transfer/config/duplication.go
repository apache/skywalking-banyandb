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

package config

import (
	"cmp"
	"fmt"
	"strings"
)

// Resume duplication policies of an import: what a resumed import does with a stream or trace
// payload that may have landed already and that count reconciliation cannot decide. Measure and
// property payloads are always pushed again: a second copy converges to the first.
const (
	// DuplicationBlock pushes nothing, lists the payloads and halts (exit 7).
	DuplicationBlock = "block"
	// DuplicationWarn pushes the payload again with a warning naming what it may duplicate.
	DuplicationWarn = "warn"
	// DuplicationPush pushes the payload again silently.
	DuplicationPush = "push"
	// DuplicationSkip leaves the payload out and forces --verify on.
	DuplicationSkip = "skip"
)

// ParseDuplicationPolicy accepts block, warn, push, skip, or "" for the default, warn.
func ParseDuplicationPolicy(raw string) (string, error) {
	switch p := strings.TrimSpace(raw); p {
	case "", DuplicationBlock, DuplicationWarn, DuplicationPush, DuplicationSkip:
		return p, nil
	default:
		return "", fmt.Errorf("unsupported resume duplication policy %q (block|warn|push|skip)", raw)
	}
}

// ResolveDuplicationPolicy is the policy of a stream or trace payload, and where it comes from:
// the policy set (by the flag, plan.yaml or the variable), else warn, the default. label names what
// set the policy (Sources.Label; "" is the flag).
func (i *Import) ResolveDuplicationPolicy(label string) (policy, source string) {
	if i.DuplicationPolicy != "" {
		return i.DuplicationPolicy, cmp.Or(label, "--resume-duplication-policy")
	}
	return DuplicationWarn, "default"
}

// HasDuplicationSkip tells whether the policy is skip: a resume may then leave payloads out,
// which a verify checks.
func (i *Import) HasDuplicationSkip() bool {
	return i.DuplicationPolicy == DuplicationSkip
}
