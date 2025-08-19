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

package sidx

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// Standard file names for sidx parts.
	primaryFilename = "primary.bin"
	dataFilename    = "data.bin"
	keysFilename    = "keys.bin"
	metaFilename    = "meta.bin"

	// Tag file extensions.
	tagDataExtension     = ".td" // <name>.td files
	tagMetadataExtension = ".tm" // <name>.tm files
	tagFilterExtension   = ".tf" // <name>.tf files
)

// part represents a collection of files containing sidx data.
// Each part contains multiple files organized by type:
// - primary.bin: Block metadata and structure information
// - data.bin: User payload data (compressed)
// - keys.bin: User-provided int64 keys (compressed)
// - meta.bin: Part metadata
// - <name>.td: Tag data files (one per tag)
// - <name>.tm: Tag metadata files (one per tag)
// - <name>.tf: Tag filter files (bloom filters, one per tag).
type part struct {
	primary       fs.Reader
	data          fs.Reader
	keys          fs.Reader
	fileSystem    fs.FileSystem
	tagData       map[string]fs.Reader
	tagMetadata   map[string]fs.Reader
	tagFilters    map[string]fs.Reader
	partMetadata  *partMetadata
	path          string
	blockMetadata []blockMetadata
}

// mustOpenPart opens a part from the specified path using the given file system.
// It opens all standard files and discovers tag files automatically.
// Panics if any required file cannot be opened.
func mustOpenPart(path string, fileSystem fs.FileSystem) *part {
	p := &part{
		path:       path,
		fileSystem: fileSystem,
	}

	// Open standard files.
	p.primary = mustOpenReader(filepath.Join(path, primaryFilename), fileSystem)
	p.data = mustOpenReader(filepath.Join(path, dataFilename), fileSystem)
	p.keys = mustOpenReader(filepath.Join(path, keysFilename), fileSystem)

	// Load part metadata from meta.bin.
	if err := p.loadPartMetadata(); err != nil {
		p.close()
		logger.GetLogger().Panic().Err(err).Str("path", path).Msg("failed to load part metadata")
	}

	// Load block metadata from primary.bin.
	if err := p.loadBlockMetadata(); err != nil {
		p.close()
		logger.GetLogger().Panic().Err(err).Str("path", path).Msg("failed to load block metadata")
	}

	// Discover and open tag files.
	p.openTagFiles()

	return p
}

// loadPartMetadata reads and parses the part metadata from meta.bin.
func (p *part) loadPartMetadata() error {
	// Read the entire meta.bin file.
	metaData, err := p.fileSystem.Read(filepath.Join(p.path, metaFilename))
	if err != nil {
		return fmt.Errorf("failed to read meta.bin: %w", err)
	}

	// Parse the metadata.
	pm, err := unmarshalPartMetadata(metaData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal part metadata: %w", err)
	}

	p.partMetadata = pm
	return nil
}

// loadBlockMetadata reads and parses block metadata from primary.bin.
func (p *part) loadBlockMetadata() error {
	// Read the entire primary.bin file.
	_, err := p.fileSystem.Read(filepath.Join(p.path, primaryFilename))
	if err != nil {
		return fmt.Errorf("failed to read primary.bin: %w", err)
	}

	// Parse block metadata (implementation would depend on the exact format).
	// For now, we'll allocate space based on the part metadata.
	p.blockMetadata = make([]blockMetadata, 0, p.partMetadata.BlocksCount)

	// TODO: Implement actual primary.bin parsing when block format is defined.
	// This is a placeholder for the structure.

	return nil
}

// openTagFiles discovers and opens all tag files in the part directory.
// Tag files follow the pattern: <name>.<extension>
// where extension is .td (data), .tm (metadata), or .tf (filter).
func (p *part) openTagFiles() {
	// Read directory entries.
	entries := p.fileSystem.ReadDir(p.path)

	// Initialize maps.
	p.tagData = make(map[string]fs.Reader)
	p.tagMetadata = make(map[string]fs.Reader)
	p.tagFilters = make(map[string]fs.Reader)

	// Process each file in the directory.
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		// Check if this is a tag file by checking for tag extensions
		if !strings.HasSuffix(fileName, tagDataExtension) &&
			!strings.HasSuffix(fileName, tagMetadataExtension) &&
			!strings.HasSuffix(fileName, tagFilterExtension) {
			continue
		}

		// Extract tag name and extension.
		tagName, extension, found := extractTagNameAndExtension(fileName)
		if !found {
			continue
		}

		// Open the appropriate reader based on extension.
		filePath := filepath.Join(p.path, fileName)
		reader := mustOpenReader(filePath, p.fileSystem)

		switch extension {
		case tagDataExtension:
			p.tagData[tagName] = reader
		case tagMetadataExtension:
			p.tagMetadata[tagName] = reader
		case tagFilterExtension:
			p.tagFilters[tagName] = reader
		default:
			// Unknown extension, close the reader.
			fs.MustClose(reader)
		}
	}
}

// extractTagNameAndExtension parses a tag filename to extract the tag name and extension.
// Expected format: <name>.<extension>
// Returns the tag name, extension, and whether parsing was successful.
func extractTagNameAndExtension(fileName string) (tagName, extension string, found bool) {
	// Find the extension.
	extIndex := strings.LastIndex(fileName, ".")
	if extIndex == -1 {
		return "", "", false
	}

	tagName = fileName[:extIndex]
	extension = fileName[extIndex:]

	// Validate extension.
	switch extension {
	case tagDataExtension, tagMetadataExtension, tagFilterExtension:
		return tagName, extension, true
	default:
		return "", "", false
	}
}

// close closes all open file readers and releases resources.
func (p *part) close() {
	if p == nil {
		return
	}
	// Close standard files.
	if p.primary != nil {
		fs.MustClose(p.primary)
	}
	if p.data != nil {
		fs.MustClose(p.data)
	}
	if p.keys != nil {
		fs.MustClose(p.keys)
	}

	// Close tag files.
	for _, reader := range p.tagData {
		fs.MustClose(reader)
	}
	for _, reader := range p.tagMetadata {
		fs.MustClose(reader)
	}
	for _, reader := range p.tagFilters {
		fs.MustClose(reader)
	}

	// Release metadata.
	if p.partMetadata != nil {
		releasePartMetadata(p.partMetadata)
		p.partMetadata = nil
	}

	// Release block metadata.
	for i := range p.blockMetadata {
		releaseBlockMetadata(&p.blockMetadata[i])
	}
	p.blockMetadata = nil
}

// mustOpenReader opens a file reader and panics if it fails.
func mustOpenReader(filePath string, fileSystem fs.FileSystem) fs.Reader {
	file, err := fileSystem.OpenFile(filePath)
	if err != nil {
		logger.GetLogger().Panic().Err(err).Str("path", filePath).Msg("cannot open file")
	}
	return file
}

// String returns a string representation of the part.
func (p *part) String() string {
	if p.partMetadata != nil {
		return fmt.Sprintf("part %d at %s", p.partMetadata.ID, p.path)
	}
	return fmt.Sprintf("part at %s", p.path)
}

// getPartMetadata returns the part metadata.
func (p *part) getPartMetadata() *partMetadata {
	return p.partMetadata
}

// getBlockMetadata returns the block metadata slice.
func (p *part) getBlockMetadata() []blockMetadata {
	return p.blockMetadata
}

// getTagDataReader returns the tag data reader for the specified tag name.
func (p *part) getTagDataReader(tagName string) (fs.Reader, bool) {
	reader, exists := p.tagData[tagName]
	return reader, exists
}

// getTagMetadataReader returns the tag metadata reader for the specified tag name.
func (p *part) getTagMetadataReader(tagName string) (fs.Reader, bool) {
	reader, exists := p.tagMetadata[tagName]
	return reader, exists
}

// getTagFilterReader returns the tag filter reader for the specified tag name.
func (p *part) getTagFilterReader(tagName string) (fs.Reader, bool) {
	reader, exists := p.tagFilters[tagName]
	return reader, exists
}

// getAvailableTagNames returns a slice of all available tag names in this part.
func (p *part) getAvailableTagNames() []string {
	tagNames := make(map[string]struct{})

	// Collect tag names from all tag file types.
	for tagName := range p.tagData {
		tagNames[tagName] = struct{}{}
	}
	for tagName := range p.tagMetadata {
		tagNames[tagName] = struct{}{}
	}
	for tagName := range p.tagFilters {
		tagNames[tagName] = struct{}{}
	}

	// Convert to slice.
	result := make([]string, 0, len(tagNames))
	for tagName := range tagNames {
		result = append(result, tagName)
	}

	return result
}

// hasTagFiles returns true if the part has any tag files for the specified tag name.
func (p *part) hasTagFiles(tagName string) bool {
	_, hasData := p.tagData[tagName]
	_, hasMeta := p.tagMetadata[tagName]
	_, hasFilter := p.tagFilters[tagName]
	return hasData || hasMeta || hasFilter
}

// Path returns the part's directory path.
func (p *part) Path() string {
	return p.path
}
