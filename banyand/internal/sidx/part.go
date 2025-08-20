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
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

const (
	// Standard file names for sidx parts.
	primaryFilename  = "primary.bin"
	dataFilename     = "data.bin"
	keysFilename     = "keys.bin"
	metaFilename     = "meta.bin"
	manifestFilename = "manifest.json"

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

// loadPartMetadata reads and parses the part metadata from manifest.json.
func (p *part) loadPartMetadata() error {
	// First try to read from manifest.json (new format)
	manifestPath := filepath.Join(p.path, manifestFilename)
	manifestData, err := p.fileSystem.Read(manifestPath)
	if err == nil {
		// Parse JSON manifest
		pm := generatePartMetadata()
		if unmarshalErr := json.Unmarshal(manifestData, pm); unmarshalErr != nil {
			releasePartMetadata(pm)
			return fmt.Errorf("failed to unmarshal manifest.json: %w", unmarshalErr)
		}
		p.partMetadata = pm
		return nil
	}

	// Fallback to meta.bin for backward compatibility
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
		return fmt.Sprintf("sidx part %d at %s", p.partMetadata.ID, p.path)
	}
	return fmt.Sprintf("sidx part at %s", p.path)
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

// memPart represents an in-memory part for SIDX with tag-based file design.
// This structure mirrors the stream module's memPart but uses user keys instead of timestamps.
type memPart struct {
	tagMetadata  map[string]*bytes.Buffer
	tagData      map[string]*bytes.Buffer
	tagFilters   map[string]*bytes.Buffer
	partMetadata *partMetadata
	meta         bytes.Buffer
	primary      bytes.Buffer
	data         bytes.Buffer
	keys         bytes.Buffer
}

// mustCreateTagWriters creates writers for individual tag files.
// Returns metadata writer, data writer, and filter writer for the specified tag.
func (mp *memPart) mustCreateTagWriters(tagName string) (fs.Writer, fs.Writer, fs.Writer) {
	if mp.tagData == nil {
		mp.tagData = make(map[string]*bytes.Buffer)
		mp.tagMetadata = make(map[string]*bytes.Buffer)
		mp.tagFilters = make(map[string]*bytes.Buffer)
	}

	// Get or create buffers for this tag
	td, tdExists := mp.tagData[tagName]
	tm := mp.tagMetadata[tagName]
	tf := mp.tagFilters[tagName]

	if tdExists {
		td.Reset()
		tm.Reset()
		tf.Reset()
		return tm, td, tf
	}

	// Create new buffers for this tag
	mp.tagData[tagName] = &bytes.Buffer{}
	mp.tagMetadata[tagName] = &bytes.Buffer{}
	mp.tagFilters[tagName] = &bytes.Buffer{}

	return mp.tagMetadata[tagName], mp.tagData[tagName], mp.tagFilters[tagName]
}

// reset clears the memory part for reuse.
func (mp *memPart) reset() {
	if mp.partMetadata != nil {
		mp.partMetadata.reset()
	}
	mp.meta.Reset()
	mp.primary.Reset()
	mp.data.Reset()
	mp.keys.Reset()

	if mp.tagData != nil {
		for k, td := range mp.tagData {
			td.Reset()
			delete(mp.tagData, k)
		}
	}
	if mp.tagMetadata != nil {
		for k, tm := range mp.tagMetadata {
			tm.Reset()
			delete(mp.tagMetadata, k)
		}
	}
	if mp.tagFilters != nil {
		for k, tf := range mp.tagFilters {
			tf.Reset()
			delete(mp.tagFilters, k)
		}
	}
}

// mustInitFromElements initializes the memory part from sorted elements using blockWriter.
func (mp *memPart) mustInitFromElements(es *elements) {
	mp.reset()

	if len(es.userKeys) == 0 {
		return
	}

	// Sort elements by seriesID first, then by user key
	sort.Sort(es)

	// Initialize part metadata
	if mp.partMetadata == nil {
		mp.partMetadata = generatePartMetadata()
	}

	// Initialize block writer for memory part
	bw := generateBlockWriter()
	defer releaseBlockWriter(bw)

	bw.MustInitForMemPart(mp)

	// Group elements by seriesID and write to blocks
	currentSeriesID := es.seriesIDs[0]
	blockStart := 0

	for i := 1; i <= len(es.seriesIDs); i++ {
		// Process block when series changes or at end
		if i == len(es.seriesIDs) || es.seriesIDs[i] != currentSeriesID {
			// Extract elements for current series
			seriesUserKeys := es.userKeys[blockStart:i]
			seriesTags := es.tags[blockStart:i]

			// Write elements for this series
			bw.MustWriteElements(currentSeriesID, seriesUserKeys, seriesTags)

			if i < len(es.seriesIDs) {
				currentSeriesID = es.seriesIDs[i]
				blockStart = i
			}
		}
	}

	// Flush the block writer to finalize metadata
	bw.Flush(mp.partMetadata)

	// Update key range in part metadata
	if len(es.userKeys) > 0 {
		mp.partMetadata.MinKey = es.userKeys[0]
		mp.partMetadata.MaxKey = es.userKeys[len(es.userKeys)-1]
	}
}

// mustFlush flushes the memory part to disk with tag-based file organization.
func (mp *memPart) mustFlush(fileSystem fs.FileSystem, partPath string) {
	fileSystem.MkdirPanicIfExist(partPath, storage.DirPerm)

	// Write core files
	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(partPath, metaFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(partPath, primaryFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.data.Buf, filepath.Join(partPath, dataFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.keys.Buf, filepath.Join(partPath, keysFilename), storage.FilePerm)

	// Write individual tag files
	for tagName, td := range mp.tagData {
		fs.MustFlush(fileSystem, td.Buf, filepath.Join(partPath, tagName+tagDataExtension), storage.FilePerm)
	}
	for tagName, tm := range mp.tagMetadata {
		fs.MustFlush(fileSystem, tm.Buf, filepath.Join(partPath, tagName+tagMetadataExtension), storage.FilePerm)
	}
	for tagName, tf := range mp.tagFilters {
		fs.MustFlush(fileSystem, tf.Buf, filepath.Join(partPath, tagName+tagFilterExtension), storage.FilePerm)
	}

	// Write part metadata manifest
	if mp.partMetadata != nil {
		manifestData, err := mp.partMetadata.marshal()
		if err != nil {
			logger.GetLogger().Panic().Err(err).Str("path", partPath).Msg("failed to marshal part metadata")
		}
		fs.MustFlush(fileSystem, manifestData, filepath.Join(partPath, manifestFilename), storage.FilePerm)
	}

	fileSystem.SyncPath(partPath)
}

// generateMemPart gets memPart from pool or creates new.
func generateMemPart() *memPart {
	v := memPartPool.Get()
	if v == nil {
		return &memPart{}
	}
	return v
}

// releaseMemPart returns memPart to pool after reset.
func releaseMemPart(mp *memPart) {
	mp.reset()
	memPartPool.Put(mp)
}

var memPartPool = pool.Register[*memPart]("sidx-memPart")

// openMemPart creates a part from a memory part.
func openMemPart(mp *memPart) *part {
	p := &part{}
	if mp.partMetadata != nil {
		// Copy part metadata
		p.partMetadata = generatePartMetadata()
		*p.partMetadata = *mp.partMetadata
	}

	// TODO: Read block metadata when blockWriter is implemented
	// p.blockMetadata = mustReadBlockMetadata(p.blockMetadata[:0], &mp.primary)

	// Open data files as readers from memory buffers
	p.primary = &mp.primary
	p.data = &mp.data
	p.keys = &mp.keys

	// Open individual tag files if they exist
	if mp.tagData != nil {
		p.tagData = make(map[string]fs.Reader)
		p.tagMetadata = make(map[string]fs.Reader)
		p.tagFilters = make(map[string]fs.Reader)

		for tagName, td := range mp.tagData {
			p.tagData[tagName] = td
			if tm, exists := mp.tagMetadata[tagName]; exists {
				p.tagMetadata[tagName] = tm
			}
			if tf, exists := mp.tagFilters[tagName]; exists {
				p.tagFilters[tagName] = tf
			}
		}
	}
	return p
}

// uncompressedElementSizeBytes calculates the uncompressed size of an element.
// This is a utility function similar to the stream module.
func uncompressedElementSizeBytes(index int, es *elements) uint64 {
	// 8 bytes for user key
	// 8 bytes for elementID
	n := uint64(8 + 8)

	// Add data payload size
	if index < len(es.data) && es.data[index] != nil {
		n += uint64(len(es.data[index]))
	}

	// Add tag sizes
	if index < len(es.tags) {
		for _, tag := range es.tags[index] {
			n += uint64(len(tag.name))
			if tag.value != nil {
				n += uint64(len(tag.value))
			}
		}
	}

	return n
}

// partPath returns the path for a part with the given epoch.
func partPath(root string, epoch uint64) string {
	return filepath.Join(root, partName(epoch))
}

// partName returns the directory name for a part with the given epoch.
// Uses 16-character hex format consistent with stream module.
func partName(epoch uint64) string {
	return fmt.Sprintf("%016x", epoch)
}
