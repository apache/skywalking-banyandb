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
	"fmt"
	"slices"
)

// Bounds of the import batch settings; a value outside them is refused, never clamped.
const (
	MinWriteBatchSize  = 1
	MaxWriteBatchSize  = 10000
	MinWriteBatchBytes = 64 << 10
	MaxWriteBatchBytes = 64 << 20
	// MinWriteInflightBatches and MaxWriteInflightBatches bound --write-inflight-batches.
	MinWriteInflightBatches = 1
	MaxWriteInflightBatches = 8
)

// Validate checks the resolved import settings against each other before anything is
// contacted, naming the source of each offending setting. Every error is a usage error
// (exit 1):
//   - --write-batch-bytes outside [64KiB, 64MiB] (the dry run uses it for its batch warnings);
//   - --write-batch-size outside [1, 10000] or --write-inflight-batches outside [1, 8] (not
//     checked under --dry-run, which ignores them);
//   - --schema-only with --include-schema=false;
//   - --verify-timeout given explicitly (flag or plan.yaml) while verification is off by
//     --verify=false or --schema-only (not checked under --dry-run, which ignores it);
//   - --verify-timeout not positive while verification is on;
//   - --force while neither the default policy nor any plan.yaml rule is overwrite.
func (i *Import) Validate(sources Sources) error {
	if i.WriteBatchBytes < MinWriteBatchBytes || i.WriteBatchBytes > MaxWriteBatchBytes {
		return fmt.Errorf("%s: %d bytes is outside [64KiB, 64MiB]", sources.Label("write-batch-bytes"), i.WriteBatchBytes)
	}
	if !i.DryRun {
		if i.WriteBatchSize < MinWriteBatchSize || i.WriteBatchSize > MaxWriteBatchSize {
			return fmt.Errorf("%s: %d is outside [%d, %d]", sources.Label("write-batch-size"), i.WriteBatchSize, MinWriteBatchSize, MaxWriteBatchSize)
		}
		if i.WriteInflightBatches < MinWriteInflightBatches || i.WriteInflightBatches > MaxWriteInflightBatches {
			return fmt.Errorf("%s: %d is outside [%d, %d]", sources.Label("write-inflight-batches"), i.WriteInflightBatches,
				MinWriteInflightBatches, MaxWriteInflightBatches)
		}
	}
	if err := CheckSchemaOnly(sources, i.SchemaOnly, i.IncludeSchema); err != nil {
		return err
	}
	if !i.DryRun && !i.EffectiveVerify() && (sources.FromFlag("verify-timeout") || sources.FromPlan("verify-timeout")) {
		return fmt.Errorf("%s has no effect: verification is off (%s=false or %s)",
			sources.Label("verify-timeout"), sources.Label("verify"), sources.Label("schema-only"))
	}
	if i.EffectiveVerify() && i.VerifyTimeout <= 0 {
		return fmt.Errorf("%s: %s must be positive", sources.Label("verify-timeout"), i.VerifyTimeout)
	}
	if i.Force && !i.HasOverwrite() {
		return fmt.Errorf("--force confirms an overwrite, but neither %s nor any rule of %s is %s",
			sources.Label("on-schema-conflict"), sources.Label("import.schemaConflictPolicy.rules"), ConflictPolicyOverwrite)
	}
	return nil
}

// CheckSchemaOnly refuses --schema-only together with --include-schema=false, since a
// schema-only transfer needs the schema. Export and import share it.
func CheckSchemaOnly(sources Sources, schemaOnly, includeSchema bool) error {
	if schemaOnly && !includeSchema {
		return fmt.Errorf("%s needs the schema; it cannot be combined with %s=false",
			sources.Label("schema-only"), sources.Label("include-schema"))
	}
	return nil
}

// EffectiveVerify reports whether the import verifies the written rows: --verify is on and
// data is written (neither --schema-only nor --dry-run).
func (i *Import) EffectiveVerify() bool {
	return i.Verify && !i.SchemaOnly && !i.DryRun
}

// HasOverwrite reports whether the default schema conflict policy or any rule is overwrite.
func (i *Import) HasOverwrite() bool {
	return i.OnSchemaConflict == ConflictPolicyOverwrite ||
		slices.ContainsFunc(i.ConflictRules, func(r ConflictRule) bool { return r.Policy == ConflictPolicyOverwrite })
}
