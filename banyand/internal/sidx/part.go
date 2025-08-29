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

	"github.com/apache/skywalking-banyandb/api/common"
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
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
// - keys.bin: User-provided int64 keys (encoded but not compressed)
// - meta.bin: Part metadata
// - <name>.td: Tag data files (one per tag)
// - <name>.tm: Tag metadata files (one per tag)
// - <name>.tf: Tag filter files (bloom filters, one per tag).
type part struct {
	primary              fs.Reader
	data                 fs.Reader
	keys                 fs.Reader
	meta                 fs.Reader
	fileSystem           fs.FileSystem
	tagData              map[string]fs.Reader
	tagMetadata          map[string]fs.Reader
	tagFilters           map[string]fs.Reader
	partMetadata         *partMetadata
	path                 string
	primaryBlockMetadata []primaryBlockMetadata
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
	p.meta = mustOpenReader(filepath.Join(path, metaFilename), fileSystem)

	// Load part metadata from meta.bin.
	if err := p.loadPartMetadata(); err != nil {
		p.close()
		logger.GetLogger().Panic().Err(err).Str("path", path).Msg("failed to load part metadata")
	}

	// Load primary block metadata from primary.bin.
	p.loadPrimaryBlockMetadata()

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

// loadPrimaryBlockMetadata reads and parses primary block metadata from meta.bin.
func (p *part) loadPrimaryBlockMetadata() {
	// Load primary block metadata from meta.bin file (compressed primaryBlockMetadata)
	p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(p.primaryBlockMetadata[:0], p.meta)
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
	if p.meta != nil {
		fs.MustClose(p.meta)
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

	// No block metadata to release since it's now passed as parameter
}

// mustOpenReader opens a file reader and panics if it fails.
func mustOpenReader(filePath string, fileSystem fs.FileSystem) fs.Reader {
	file, err := fileSystem.OpenFile(filePath)
	if err != nil {
		logger.GetLogger().Panic().Err(err).Str("path", filePath).Msg("cannot open file")
	}
	return file
}

// readAll reads all blocks from the part and returns them as separate elements.
// Each elements collection represents the data from a single block.
func (p *part) readAll() ([]*elements, error) {
	if len(p.primaryBlockMetadata) == 0 {
		return nil, nil
	}

	result := make([]*elements, 0, len(p.primaryBlockMetadata))
	compressedPrimaryBuf := make([]byte, 0, 1024)
	primaryBuf := make([]byte, 0, 1024)
	compressedDataBuf := make([]byte, 0, 1024)
	dataBuf := make([]byte, 0, 1024)
	compressedKeysBuf := make([]byte, 0, 1024)

	for _, pbm := range p.primaryBlockMetadata {
		// Read and decompress primary block metadata
		compressedPrimaryBuf = bytes.ResizeOver(compressedPrimaryBuf, int(pbm.size))
		fs.MustReadData(p.primary, int64(pbm.offset), compressedPrimaryBuf)

		var err error
		primaryBuf, err = zstd.Decompress(primaryBuf[:0], compressedPrimaryBuf)
		if err != nil {
			// Clean up any elements created so far
			for _, e := range result {
				releaseElements(e)
			}
			return nil, fmt.Errorf("cannot decompress primary block: %w", err)
		}

		// Unmarshal all block metadata entries in this primary block
		blockMetadataArray, err := unmarshalBlockMetadata(nil, primaryBuf)
		if err != nil {
			// Clean up any elements created so far
			for _, e := range result {
				releaseElements(e)
			}
			return nil, fmt.Errorf("cannot unmarshal block metadata: %w", err)
		}

		// Process each block metadata
		for i := range blockMetadataArray {
			bm := &blockMetadataArray[i]

			// Create elements for this block
			elems := generateElements()

			// Read user keys
			compressedKeysBuf = bytes.ResizeOver(compressedKeysBuf, int(bm.keysBlock.size))
			fs.MustReadData(p.keys, int64(bm.keysBlock.offset), compressedKeysBuf)

			// Decode user keys directly using the stored encoding information
			elems.userKeys, err = encoding.BytesToInt64List(elems.userKeys[:0], compressedKeysBuf, bm.keysEncodeType, bm.minKey, int(bm.count))
			if err != nil {
				releaseElements(elems)
				for _, e := range result {
					releaseElements(e)
				}
				return nil, fmt.Errorf("cannot decode user keys: %w", err)
			}

			// Read data payloads
			compressedDataBuf = bytes.ResizeOver(compressedDataBuf, int(bm.dataBlock.size))
			fs.MustReadData(p.data, int64(bm.dataBlock.offset), compressedDataBuf)

			dataBuf, err = zstd.Decompress(dataBuf[:0], compressedDataBuf)
			if err != nil {
				releaseElements(elems)
				for _, e := range result {
					releaseElements(e)
				}
				return nil, fmt.Errorf("cannot decompress data block: %w", err)
			}

			// Decode data payloads - create a new decoder for each block to avoid state corruption
			blockBytesDecoder := &encoding.BytesBlockDecoder{}
			elems.data, err = blockBytesDecoder.Decode(elems.data[:0], dataBuf, bm.count)
			if err != nil {
				releaseElements(elems)
				for _, e := range result {
					releaseElements(e)
				}
				return nil, fmt.Errorf("cannot decode data payloads: %w", err)
			}

			// Initialize seriesIDs and tags slices
			elems.seriesIDs = make([]common.SeriesID, int(bm.count))
			elems.tags = make([][]*tag, int(bm.count))

			// Fill seriesIDs - all elements in this block have the same seriesID
			for j := range elems.seriesIDs {
				elems.seriesIDs[j] = bm.seriesID
			}

			// Read tags for each tag name
			for tagName := range bm.tagsBlocks {
				err = p.readBlockTags(tagName, bm, elems, blockBytesDecoder)
				if err != nil {
					releaseElements(elems)
					for _, e := range result {
						releaseElements(e)
					}
					return nil, fmt.Errorf("cannot read tags for %s: %w", tagName, err)
				}
			}

			result = append(result, elems)
		}
	}

	return result, nil
}

// readBlockTags reads and decodes tag data for a specific tag in a block.
func (p *part) readBlockTags(tagName string, bm *blockMetadata, elems *elements, decoder *encoding.BytesBlockDecoder) error {
	tagBlockInfo, exists := bm.tagsBlocks[tagName]
	if !exists {
		return fmt.Errorf("tag block info not found for tag: %s", tagName)
	}

	// Get tag metadata reader
	tmReader, tmExists := p.getTagMetadataReader(tagName)
	if !tmExists {
		return fmt.Errorf("tag metadata reader not found for tag: %s", tagName)
	}

	// Get tag data reader
	tdReader, tdExists := p.getTagDataReader(tagName)
	if !tdExists {
		return fmt.Errorf("tag data reader not found for tag: %s", tagName)
	}

	// Read tag metadata
	tmData := make([]byte, tagBlockInfo.size)
	fs.MustReadData(tmReader, int64(tagBlockInfo.offset), tmData)

	tm, err := unmarshalTagMetadata(tmData)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag metadata: %w", err)
	}
	defer releaseTagMetadata(tm)

	// Read tag data
	tdData := make([]byte, tm.dataBlock.size)
	fs.MustReadData(tdReader, int64(tm.dataBlock.offset), tdData)

	// Decode tag values directly (no compression)
	tagValues, err := internalencoding.DecodeTagValues(nil, decoder, &bytes.Buffer{Buf: tdData}, tm.valueType, int(bm.count))
	if err != nil {
		return fmt.Errorf("cannot decode tag values: %w", err)
	}

	// Add tag values to elements (only for non-nil values)
	for i, value := range tagValues {
		if i >= len(elems.tags) {
			break
		}
		// Skip nil values - they represent missing tags for this element
		if value == nil {
			continue
		}
		if elems.tags[i] == nil {
			elems.tags[i] = make([]*tag, 0, 1)
		}
		newTag := generateTag()
		newTag.name = tagName
		newTag.value = value
		newTag.valueType = tm.valueType
		newTag.indexed = tm.indexed
		elems.tags[i] = append(elems.tags[i], newTag)
	}

	return nil
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
			seriesData := es.data[blockStart:i]
			seriesTags := es.tags[blockStart:i]

			// Write elements for this series
			bw.MustWriteElements(currentSeriesID, seriesUserKeys, seriesData, seriesTags)

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
	fs.MustFlush(fileSystem, mp.primary.Buf, filepath.Join(partPath, primaryFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.data.Buf, filepath.Join(partPath, dataFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.keys.Buf, filepath.Join(partPath, keysFilename), storage.FilePerm)
	fs.MustFlush(fileSystem, mp.meta.Buf, filepath.Join(partPath, metaFilename), storage.FilePerm)

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

	// Load primary block metadata from meta buffer
	if len(mp.meta.Buf) > 0 {
		p.primaryBlockMetadata = mustReadPrimaryBlockMetadata(nil, &mp.meta)
	}

	// Open data files as readers from memory buffers
	p.primary = &mp.primary
	p.data = &mp.data
	p.keys = &mp.keys
	p.meta = &mp.meta

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

// partPath returns the path for a part with the given epoch.
func partPath(root string, epoch uint64) string {
	return filepath.Join(root, partName(epoch))
}

// partName returns the directory name for a part with the given epoch.
// Uses 16-character hex format consistent with stream module.
func partName(epoch uint64) string {
	return fmt.Sprintf("%016x", epoch)
}
