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
	"os"
	"path/filepath"
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
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
	// MinPartChunkSize and MaxPartChunkSize bound --part-chunk-size; the maximum is the wire
	// limit of ImportPartsRequest.Chunk, which the liaison enforces.
	MinPartChunkSize = 64 << 10
	MaxPartChunkSize = nativefmt.MaxChunkSize
	// MaxCheckpointEveryBatches bounds --checkpoint-every-batches.
	MaxCheckpointEveryBatches = 10000
)

// Validate checks the resolved import settings against each other before anything is
// contacted, naming the source of each offending setting. Every error is a usage error
// (exit 1):
//   - --write-batch-bytes outside [MinWriteBatchBytes, MaxWriteBatchBytes] (the dry run uses it
//     for its batch warnings);
//   - --part-chunk-size outside [MinPartChunkSize, MaxPartChunkSize];
//   - --write-batch-size outside [MinWriteBatchSize, MaxWriteBatchSize], --write-inflight-batches
//     outside [MinWriteInflightBatches, MaxWriteInflightBatches] or --checkpoint-every-batches
//     outside [1, MaxCheckpointEveryBatches] (not checked under --dry-run, which ignores them);
//   - --schema-only with --include-schema=false;
//   - --verify-timeout given explicitly (flag or plan.yaml) with --schema-only (not checked
//     under --dry-run, which ignores it); with --verify=false it is accepted, because a skip
//     duplication policy, or a payload an earlier run skipped and the progress file records,
//     forces the verification on;
//   - --verify-timeout not positive while data is written (neither --schema-only nor --dry-run),
//     whether verification is on or can be forced on;
//   - --force while neither the default policy nor any plan.yaml rule is overwrite;
//   - --restart with the --resume flag, unless --dry-run or --schema-only, which ignore --resume; a
//     resume from BYDBCTL_IMPORT_RESUME yields to --restart instead, and is turned off;
func (i *Import) Validate(sources Sources) error {
	if i.WriteBatchBytes < MinWriteBatchBytes || i.WriteBatchBytes > MaxWriteBatchBytes {
		return fmt.Errorf("%s: %d bytes is outside [%s, %s]", sources.Label(&i.WriteBatchBytes), i.WriteBatchBytes,
			sizeLabel(MinWriteBatchBytes), sizeLabel(MaxWriteBatchBytes))
	}
	if i.PartChunkSize < MinPartChunkSize || i.PartChunkSize > MaxPartChunkSize {
		return fmt.Errorf("%s: %d bytes is outside [%s, %s]", sources.Label(&i.PartChunkSize), i.PartChunkSize,
			sizeLabel(MinPartChunkSize), sizeLabel(MaxPartChunkSize))
	}
	if !i.DryRun {
		if i.WriteBatchSize < MinWriteBatchSize || i.WriteBatchSize > MaxWriteBatchSize {
			return fmt.Errorf("%s: %d is outside [%d, %d]", sources.Label(&i.WriteBatchSize), i.WriteBatchSize, MinWriteBatchSize, MaxWriteBatchSize)
		}
		if i.CheckpointEveryBatches < 1 || i.CheckpointEveryBatches > MaxCheckpointEveryBatches {
			return fmt.Errorf("%s: %d is outside [1, %d]", sources.Label(&i.CheckpointEveryBatches), i.CheckpointEveryBatches, MaxCheckpointEveryBatches)
		}
		if i.WriteInflightBatches < MinWriteInflightBatches || i.WriteInflightBatches > MaxWriteInflightBatches {
			return fmt.Errorf("%s: %d is outside [%d, %d]", sources.Label(&i.WriteInflightBatches), i.WriteInflightBatches,
				MinWriteInflightBatches, MaxWriteInflightBatches)
		}
	}
	if err := CheckSchemaOnly(sources, &i.SchemaOnly, &i.IncludeSchema); err != nil {
		return err
	}
	if i.Restart && i.Resume {
		if !sources.FromEnv(&i.Resume) && !i.DryRun && !i.SchemaOnly {
			return fmt.Errorf("%s and %s exclude each other: --restart starts the import over, --resume continues it", sources.Label(&i.Restart), sources.Label(&i.Resume))
		}
		// The variable is a default for every run; the flag asks for this one.
		i.Resume = false
	}
	if i.SchemaOnly && !i.DryRun && sources.Explicit(&i.VerifyTimeout) {
		return fmt.Errorf("%s has no effect: %s writes no data to verify", sources.Label(&i.VerifyTimeout), sources.Label(&i.SchemaOnly))
	}
	if !i.SchemaOnly && !i.DryRun && i.VerifyTimeout <= 0 {
		return fmt.Errorf("%s: %s must be positive; even with %s=false a skipped payload forces the verification on",
			sources.Label(&i.VerifyTimeout), i.VerifyTimeout, sources.Label(&i.Verify))
	}
	if i.Force && !i.HasOverwrite() {
		return fmt.Errorf("--force confirms an overwrite, but neither %s nor any rule of %s is %s",
			sources.Label(&i.OnSchemaConflict), sources.Label(&i.ConflictRules), ConflictPolicyOverwrite)
	}
	return nil
}

// CheckCheckpoint refuses a checkpoint setting (the --checkpoint field) that names a directory,
// or dir (the artifact or output directory) itself, rather than a progress file; fileName is the
// progress file the message suggests below it. Export and import share it.
func CheckCheckpoint(sources Sources, checkpointSetting *string, dir, fileName string) error {
	checkpoint := *checkpointSetting
	if checkpoint == "" {
		return nil
	}
	fi, err := os.Stat(checkpoint)
	isDir := err == nil && fi.IsDir()
	if !isDir && (dir == "" || !SameDir(checkpoint, dir)) {
		return nil
	}
	// dir itself may not exist yet.
	what := "names a directory"
	if isDir {
		what = "is a directory"
	}
	return fmt.Errorf("%s %s %s; it names the progress file, such as %s", sources.Label(checkpointSetting), checkpoint, what,
		filepath.Join(checkpoint, fileName))
}

// SameDir reports whether a and b name the same directory, however they are spelled.
func SameDir(a, b string) bool {
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	if errA == nil && errB == nil && absA == absB {
		return true
	}
	infoA, errA := os.Stat(a)
	infoB, errB := os.Stat(b)
	return errA == nil && errB == nil && os.SameFile(infoA, infoB)
}

// CheckSchemaOnly refuses --schema-only together with --include-schema=false, given as the two
// fields, since a schema-only transfer needs the schema. Export and import share it.
func CheckSchemaOnly(sources Sources, schemaOnly, includeSchema *bool) error {
	if *schemaOnly && !*includeSchema {
		return fmt.Errorf("%s needs the schema; it cannot be combined with %s=false",
			sources.Label(schemaOnly), sources.Label(includeSchema))
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

// sizeLabel prints a byte size in the largest binary unit that divides it.
func sizeLabel(n uint64) string {
	switch {
	case n != 0 && n%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", n>>20)
	case n != 0 && n%(1<<10) == 0:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}
