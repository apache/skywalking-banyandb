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

// Package dump provides utilities for dumping BanyanDB data.
package dump

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// TryOpenSeriesMetadata attempts to open the series metadata file (smeta.bin) in the given part path.
// It returns the reader if successful, or nil if the file doesn't exist or on error.
// Only file not found errors are silently ignored; other errors are reported as warnings.
func TryOpenSeriesMetadata(fileSystem fs.FileSystem, partPath string) fs.Reader {
	seriesMetadataPath := filepath.Join(partPath, "smeta.bin")
	reader, err := fileSystem.OpenFile(seriesMetadataPath)
	if err != nil {
		// Only ignore file not found errors; other errors should be reported
		var fsErr *fs.FileSystemError
		if !errors.As(err, &fsErr) || fsErr.Code != fs.IsNotExistError {
			fmt.Fprintf(os.Stderr, "Warning: Failed to open series metadata file %s: %v\n", seriesMetadataPath, err)
		}
		// File doesn't exist, it's okay - just continue without it
		return nil
	}
	return reader
}

// ParseSeriesMetadata parses series metadata from the given reader and stores EntityValues in partSeriesMap.
// It reads all data from the series metadata file, unmarshals Documents, and stores the mapping
// of SeriesID to EntityValues for use in CSV output.
func ParseSeriesMetadata(partID uint64, seriesMetadata fs.Reader, partSeriesMap map[uint64]map[common.SeriesID]string) error {
	// Read all data from series metadata file
	seqReader := seriesMetadata.SequentialRead()
	defer seqReader.Close()

	readMetadataBytes, err := io.ReadAll(seqReader)
	if err != nil {
		return fmt.Errorf("failed to read series metadata: %w", err)
	}

	if len(readMetadataBytes) == 0 {
		return nil // Empty file, nothing to parse
	}

	// Unmarshal Documents
	var docs index.Documents
	if err := docs.Unmarshal(readMetadataBytes); err != nil {
		return fmt.Errorf("failed to unmarshal series metadata: %w", err)
	}

	if len(docs) == 0 {
		return nil // No documents
	}

	// Store EntityValues in partSeriesMap for use in CSV output
	partMap := make(map[common.SeriesID]string)
	for _, doc := range docs {
		seriesID := common.SeriesID(convert.Hash(doc.EntityValues))
		partMap[seriesID] = string(doc.EntityValues)
	}
	partSeriesMap[partID] = partMap

	return nil
}
