// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/fileformat"
	"github.com/apache/skywalking-banyandb/pkg/initerror"
)

const (
	metadataFilename = "metadata"
	currentVersion   = fileformat.CurrentVersion
)

// SegmentMetadataFilename is the filename used for the per-segment
// metadata document. Exported so out-of-package tooling (e.g. the
// migration copy CLI) writes the file to the exact location runtime
// readers expect.
const SegmentMetadataFilename = metadataFilename

// PartMetadataFilename is the per-part metadata document stream, measure and trace write
// under <shard>/<part>/. Exported for the same reason as SegmentMetadataFilename.
const PartMetadataFilename = partDiskMetadataFilename

// CurrentSegmentVersion is the version string a freshly-written segment
// must carry. Exported for the same reason as SegmentMetadataFilename.
const CurrentSegmentVersion = currentVersion

// SegmentMetadata is the on-disk shape of <segment>/metadata, shared by the runtime
// reader and out-of-package writers so both use the same JSON tags (lowercase
// `version` / `endTime,omitempty`).
type SegmentMetadata struct {
	Version string `json:"version"`
	EndTime string `json:"endTime,omitempty"`
}

var errVersionIncompatible = errors.New("version not compatible")

var compatibleVersions = fileformat.CompatibleVersions()

func checkVersion(version string) error {
	for _, v := range compatibleVersions {
		if v == version {
			return nil
		}
	}
	return initerror.AsPermanent(errors.WithMessagef(errVersionIncompatible,
		"incompatible version %s, supported versions: %s", version, strings.Join(compatibleVersions, ", ")))
}

// ErrEmptySegmentMetadata is returned (wrapped) by DecodeSegmentMetadata for a metadata
// file without content, e.g. one created but not yet written by a rollover.
var ErrEmptySegmentMetadata = errors.New("segment metadata is empty")

// DecodeSegmentMetadata parses the raw content of a <segment>/metadata file into
// SegmentMetadata. It accepts both the JSON document written by current releases and
// the legacy bare version string, and performs no compatibility check: callers that
// need to reject unsupported versions go through readSegmentMeta.
func DecodeSegmentMetadata(data []byte) (SegmentMetadata, error) {
	var meta SegmentMetadata
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return SegmentMetadata{}, errors.WithStack(ErrEmptySegmentMetadata)
	}
	if trimmed[0] == '{' {
		if err := json.Unmarshal(data, &meta); err != nil {
			return SegmentMetadata{}, err
		}
		return meta, nil
	}
	meta.Version = trimmed
	return meta, nil
}

func readSegmentMeta(data []byte) (SegmentMetadata, error) {
	decoded, err := DecodeSegmentMetadata(data)
	if err != nil {
		if errors.Is(err, ErrEmptySegmentMetadata) {
			// Empty metadata carries no version: as incompatible as an unknown one, so retrying cannot help.
			return SegmentMetadata{}, initerror.AsPermanent(err)
		}
		return SegmentMetadata{}, err
	}
	if checkErr := checkVersion(decoded.Version); checkErr != nil {
		return SegmentMetadata{}, checkErr
	}
	return decoded, nil
}

// GetCurrentVersion returns the current storage version.
func GetCurrentVersion() string {
	return currentVersion
}

// GetCompatibleVersions returns the list of compatible storage versions.
func GetCompatibleVersions() []string {
	return compatibleVersions
}
