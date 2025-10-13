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

package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// handoffController manages handoff queues for multiple data nodes.
type handoffController struct {
	l          *logger.Logger
	fileSystem fs.FileSystem
	nodeQueues map[string]*handoffNodeQueue // key: node address
	root       string                       // <root>/handoff/nodes/
	mu         sync.RWMutex

	// Node status monitoring and replay control
	tire2Client        queueClient
	allDataNodes       []string            // All configured data nodes
	healthyNodes       map[string]struct{} // Currently healthy nodes
	statusChangeChan   chan nodeStatusChange
	stopMonitor        chan struct{}
	monitorWg          sync.WaitGroup
	checkInterval      time.Duration
	replayWg           sync.WaitGroup
	replayStopChan     chan struct{}
	replayTriggerChan  chan string                    // Node addresses to trigger replay
	inFlightSends      map[string]map[uint64]struct{} // node -> set of partIDs being sent
	inFlightMu         sync.RWMutex
	replayBatchSize    int           // Parts per node per round
	replayPollInterval time.Duration // How often to check for work

	// Size tracking for volume control
	maxTotalSizeBytes uint64       // Maximum total size across all nodes
	currentTotalSize  uint64       // Current total size across all nodes
	sizeMu            sync.RWMutex // Protects size tracking
}

// nodeStatusChange represents a node status transition.
type nodeStatusChange struct {
	nodeName string
	isOnline bool
}

// queueClient interface combines health checking and sync client creation
type queueClient interface {
	HealthyNodes() []string
	NewChunkedSyncClient(node string, chunkSize uint32) (queue.ChunkedSyncClient, error)
}

// newHandoffController creates a new handoff controller.
func newHandoffController(fileSystem fs.FileSystem, root string, tire2Client queueClient,
	dataNodeList []string, maxSizeMB int, l *logger.Logger) (*handoffController, error) {
	handoffRoot := filepath.Join(root, "handoff", "nodes")

	hc := &handoffController{
		l:                  l,
		fileSystem:         fileSystem,
		nodeQueues:         make(map[string]*handoffNodeQueue),
		root:               handoffRoot,
		tire2Client:        tire2Client,
		allDataNodes:       dataNodeList,
		healthyNodes:       make(map[string]struct{}),
		statusChangeChan:   make(chan nodeStatusChange, 100),
		stopMonitor:        make(chan struct{}),
		checkInterval:      5 * time.Second,
		replayStopChan:     make(chan struct{}),
		replayTriggerChan:  make(chan string, 100),
		inFlightSends:      make(map[string]map[uint64]struct{}),
		replayBatchSize:    10,
		replayPollInterval: 1 * time.Second,
		maxTotalSizeBytes:  uint64(maxSizeMB) * 1024 * 1024, // Convert MB to bytes
		currentTotalSize:   0,
	}

	// Create handoff root directory if it doesn't exist
	fileSystem.MkdirIfNotExist(handoffRoot, storage.DirPerm)

	// Load existing node queues from disk
	if err := hc.loadExistingQueues(); err != nil {
		l.Warn().Err(err).Msg("failed to load existing handoff queues")
	}

	// Start the node status monitor
	hc.startMonitor()

	// Start the replay worker
	hc.startReplayWorker()

	return hc, nil
}

// loadExistingQueues scans the handoff directory and loads existing node queues.
func (hc *handoffController) loadExistingQueues() error {
	entries := hc.fileSystem.ReadDir(hc.root)
	var totalRecoveredSize uint64

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		nodeRoot := filepath.Join(hc.root, entry.Name())

		// Read the original node address from .node_info file
		nodeAddr, err := readNodeInfo(hc.fileSystem, nodeRoot)
		if err != nil {
			hc.l.Warn().Err(err).Str("dir", entry.Name()).Msg("failed to read node info, skipping")
			continue
		}

		nodeQueue, err := newHandoffNodeQueue(nodeAddr, nodeRoot, hc.fileSystem, hc.l)
		if err != nil {
			hc.l.Warn().Err(err).Str("node", nodeAddr).Msg("failed to load node queue")
			continue
		}

		hc.nodeQueues[nodeAddr] = nodeQueue

		// Calculate queue size from metadata (not from actual file sizes)
		pending, _ := nodeQueue.listPending()
		var queueSize uint64
		for _, ptp := range pending {
			meta, err := nodeQueue.getMetadata(ptp.PartID, ptp.PartType)
			if err == nil && meta.PartSizeBytes > 0 {
				queueSize += meta.PartSizeBytes
			}
		}
		totalRecoveredSize += queueSize

		// Log pending parts count
		if len(pending) > 0 {
			hc.l.Info().
				Str("node", nodeAddr).
				Int("pending", len(pending)).
				Uint64("sizeMB", queueSize/1024/1024).
				Msg("loaded handoff queue with pending parts")
		}
	}

	// Update current total size
	hc.currentTotalSize = totalRecoveredSize
	if totalRecoveredSize > 0 {
		hc.l.Info().
			Uint64("totalSizeMB", totalRecoveredSize/1024/1024).
			Int("nodeCount", len(hc.nodeQueues)).
			Msg("recovered handoff queue state")
	}

	return nil
}

// enqueueForNode adds a part to the handoff queue for a specific node.
func (hc *handoffController) enqueueForNode(nodeAddr string, partID uint64, partType string, sourcePath string,
	group string, shardID uint32) error {
	// Read part size from metadata
	partSize := hc.readPartSizeFromMetadata(sourcePath, partType)

	// Check if enqueue would exceed limit
	if !hc.canEnqueue(partSize) {
		currentSize := hc.getTotalSize()
		return fmt.Errorf("handoff queue full: current=%d MB, limit=%d MB, part=%d MB",
			currentSize/1024/1024, hc.maxTotalSizeBytes/1024/1024, partSize/1024/1024)
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            group,
		ShardID:          shardID,
		PartType:         partType,
		PartSizeBytes:    partSize,
	}

	nodeQueue, err := hc.getOrCreateNodeQueue(nodeAddr)
	if err != nil {
		return fmt.Errorf("failed to get node queue for %s: %w", nodeAddr, err)
	}

	if err := nodeQueue.enqueue(partID, partType, sourcePath, meta); err != nil {
		return err
	}

	// Update total size after successful enqueue
	hc.updateTotalSize(int64(partSize))

	return nil
}

// enqueueForNodes adds a part to the handoff queues for multiple offline nodes.
func (hc *handoffController) enqueueForNodes(offlineNodes []string, partID uint64, partType string, sourcePath string,
	group string, shardID uint32) error {
	meta := &handoffMetadata{
		EnqueueTimestamp: time.Now().UnixNano(),
		Group:            group,
		ShardID:          shardID,
		PartType:         partType,
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	var firstErr error
	successCount := 0

	// For each offline node, create hardlinked copy
	for _, nodeAddr := range offlineNodes {
		nodeQueue, err := hc.getOrCreateNodeQueue(nodeAddr)
		if err != nil {
			hc.l.Error().Err(err).Str("node", nodeAddr).Msg("failed to get node queue")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		if err := nodeQueue.enqueue(partID, partType, sourcePath, meta); err != nil {
			hc.l.Error().Err(err).Str("node", nodeAddr).Uint64("partId", partID).Str("partType", partType).
				Msg("failed to enqueue part")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		successCount++
	}

	if successCount > 0 {
		hc.l.Info().
			Int("successCount", successCount).
			Int("totalNodes", len(offlineNodes)).
			Uint64("partId", partID).
			Str("partType", partType).
			Msg("part enqueued to handoff queues")
	}

	// Return error only if all enqueues failed
	if successCount == 0 && firstErr != nil {
		return firstErr
	}

	return nil
}

// getOrCreateNodeQueue gets an existing node queue or creates a new one.
// Caller must hold hc.mu lock.
func (hc *handoffController) getOrCreateNodeQueue(nodeAddr string) (*handoffNodeQueue, error) {
	// Check if queue already exists
	if queue, exists := hc.nodeQueues[nodeAddr]; exists {
		return queue, nil
	}

	// Create new queue
	sanitizedAddr := sanitizeNodeAddr(nodeAddr)
	nodeRoot := filepath.Join(hc.root, sanitizedAddr)

	nodeQueue, err := newHandoffNodeQueue(nodeAddr, nodeRoot, hc.fileSystem, hc.l)
	if err != nil {
		return nil, fmt.Errorf("failed to create node queue: %w", err)
	}

	hc.nodeQueues[nodeAddr] = nodeQueue
	return nodeQueue, nil
}

// listPendingForNode returns all pending parts with their types for a specific node.
func (hc *handoffController) listPendingForNode(nodeAddr string) ([]partTypePair, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	if !exists {
		return nil, nil // No queue means no pending parts
	}

	return nodeQueue.listPending()
}

// getPartPath returns the path to a specific part type directory in a node's handoff queue.
func (hc *handoffController) getPartPath(nodeAddr string, partID uint64, partType string) string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	if !exists {
		return ""
	}

	return nodeQueue.getPartTypePath(partID, partType)
}

// getPartMetadata returns the handoff metadata for a specific part type.
func (hc *handoffController) getPartMetadata(nodeAddr string, partID uint64, partType string) (*handoffMetadata, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	if !exists {
		return nil, fmt.Errorf("node queue not found for %s", nodeAddr)
	}

	return nodeQueue.getMetadata(partID, partType)
}

// completeSend removes a specific part type from a node's handoff queue after successful delivery.
func (hc *handoffController) completeSend(nodeAddr string, partID uint64, partType string) error {
	// Get part size before removing
	var partSize uint64
	meta, err := hc.getPartMetadata(nodeAddr, partID, partType)
	if err == nil && meta.PartSizeBytes > 0 {
		partSize = meta.PartSizeBytes
	}

	hc.mu.RLock()
	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	hc.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node queue not found for %s", nodeAddr)
	}

	if err := nodeQueue.complete(partID, partType); err != nil {
		return err
	}

	// Update total size after successful removal
	if partSize > 0 {
		hc.updateTotalSize(-int64(partSize))
	}

	return nil
}

// completeSendAll removes all part types for a given partID from a node's handoff queue.
func (hc *handoffController) completeSendAll(nodeAddr string, partID uint64) error {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	if !exists {
		return fmt.Errorf("node queue not found for %s", nodeAddr)
	}

	return nodeQueue.completeAll(partID)
}

// getNodeQueueSize returns the total size of pending parts for a specific node.
func (hc *handoffController) getNodeQueueSize(nodeAddr string) (uint64, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodeQueue, exists := hc.nodeQueues[nodeAddr]
	if !exists {
		return 0, nil
	}

	return nodeQueue.size()
}

// getAllNodeQueues returns a snapshot of all node addresses with handoff queues.
func (hc *handoffController) getAllNodeQueues() []string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	nodes := make([]string, 0, len(hc.nodeQueues))
	for nodeAddr := range hc.nodeQueues {
		nodes = append(nodes, nodeAddr)
	}

	return nodes
}

// partInfo contains information about a part to be enqueued.
type partInfo struct {
	partID  uint64
	path    string
	group   string
	shardID common.ShardID
}

// calculateOfflineNodes returns the list of offline nodes by comparing online nodes with all configured nodes.
func (hc *handoffController) calculateOfflineNodes(onlineNodes []string) []string {
	if hc == nil || len(hc.allDataNodes) == 0 {
		return nil
	}

	// Convert onlineNodes to a set for O(1) lookup
	onlineSet := make(map[string]struct{}, len(onlineNodes))
	for _, node := range onlineNodes {
		onlineSet[node] = struct{}{}
	}

	// Calculate offline nodes: allDataNodes - onlineNodes
	var offlineNodes []string
	for _, node := range hc.allDataNodes {
		if _, isOnline := onlineSet[node]; !isOnline {
			offlineNodes = append(offlineNodes, node)
		}
	}

	return offlineNodes
}

// enqueueForOfflineNodes enqueues parts for the given offline nodes.
// offlineNodes: list of offline nodes to enqueue parts for
// coreParts: slice of core part information
// sidxParts: map of sidxName -> part information
func (hc *handoffController) enqueueForOfflineNodes(
	offlineNodes []string,
	coreParts []partInfo,
	sidxParts map[string][]partInfo,
) error {
	if hc == nil || len(offlineNodes) == 0 {
		return nil
	}

	// Track enqueue statistics
	totalCoreEnqueued := 0
	totalSidxEnqueued := 0
	var lastErr error

	// For each offline node, enqueue all parts
	for _, nodeAddr := range offlineNodes {
		// Enqueue core parts
		for _, coreInfo := range coreParts {
			err := hc.enqueueForNode(nodeAddr, coreInfo.partID, PartTypeCore, coreInfo.path, coreInfo.group, uint32(coreInfo.shardID))
			if err != nil {
				hc.l.Warn().Err(err).
					Str("node", nodeAddr).
					Uint64("partID", coreInfo.partID).
					Msg("failed to enqueue core part")
				lastErr = err
			} else {
				totalCoreEnqueued++
			}
		}

		// Enqueue sidx parts
		for sidxName, parts := range sidxParts {
			for _, sidxInfo := range parts {
				err := hc.enqueueForNode(nodeAddr, sidxInfo.partID, sidxName, sidxInfo.path, sidxInfo.group, uint32(sidxInfo.shardID))
				if err != nil {
					hc.l.Warn().Err(err).
						Str("node", nodeAddr).
						Str("sidx", sidxName).
						Uint64("partID", sidxInfo.partID).
						Msg("failed to enqueue sidx part")
					lastErr = err
				} else {
					totalSidxEnqueued++
				}
			}
		}
	}

	// Log summary
	hc.l.Info().
		Int("offlineNodes", len(offlineNodes)).
		Int("corePartsEnqueued", totalCoreEnqueued).
		Int("sidxPartsEnqueued", totalSidxEnqueued).
		Msg("enqueued parts for offline nodes")

	return lastErr
}

// close closes the handoff controller.
func (hc *handoffController) close() error {
	// Stop the monitor
	if hc.stopMonitor != nil {
		close(hc.stopMonitor)
		hc.monitorWg.Wait()
	}

	// Stop the replay worker
	if hc.replayStopChan != nil {
		close(hc.replayStopChan)
		hc.replayWg.Wait()
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// Clear node queues
	hc.nodeQueues = nil

	// Clear in-flight tracking
	hc.inFlightSends = nil

	return nil
}

// getTotalSize returns the current total size across all node queues.
func (hc *handoffController) getTotalSize() uint64 {
	hc.sizeMu.RLock()
	defer hc.sizeMu.RUnlock()
	return hc.currentTotalSize
}

// canEnqueue checks if adding a part of the given size would exceed the total size limit.
func (hc *handoffController) canEnqueue(partSize uint64) bool {
	if hc.maxTotalSizeBytes == 0 {
		return true // No limit configured
	}

	hc.sizeMu.RLock()
	defer hc.sizeMu.RUnlock()
	return hc.currentTotalSize+partSize <= hc.maxTotalSizeBytes
}

// readPartSizeFromMetadata reads the CompressedSizeBytes from the part's metadata file.
func (hc *handoffController) readPartSizeFromMetadata(sourcePath, partType string) uint64 {
	var metadataPath string

	// Core parts use metadata.json, sidx parts use manifest.json
	if partType == PartTypeCore {
		metadataPath = filepath.Join(sourcePath, metadataFilename) // "metadata.json"
	} else {
		metadataPath = filepath.Join(sourcePath, "manifest.json")
	}

	// Read metadata file
	data, err := hc.fileSystem.Read(metadataPath)
	if err != nil {
		hc.l.Warn().Err(err).Str("path", metadataPath).Msg("failed to read metadata file")
		return 0
	}

	// Parse metadata to get CompressedSizeBytes
	var metadata struct {
		CompressedSizeBytes uint64 `json:"compressedSizeBytes"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		hc.l.Warn().Err(err).Str("path", metadataPath).Msg("failed to parse metadata")
		return 0
	}

	return metadata.CompressedSizeBytes
}

// updateTotalSize atomically updates the current total size.
func (hc *handoffController) updateTotalSize(delta int64) {
	hc.sizeMu.Lock()
	defer hc.sizeMu.Unlock()

	if delta > 0 {
		// Enqueue: add to total
		hc.currentTotalSize += uint64(delta)
	} else if delta < 0 {
		// Complete: subtract from total with underflow check
		toSubtract := uint64(-delta)
		if toSubtract > hc.currentTotalSize {
			hc.l.Warn().
				Uint64("current", hc.currentTotalSize).
				Uint64("toSubtract", toSubtract).
				Msg("attempted to subtract more than current size, resetting to 0")
			hc.currentTotalSize = 0
		} else {
			hc.currentTotalSize -= toSubtract
		}
	}
}

// sanitizeNodeAddr converts a node address to a safe directory name.
// It replaces colons and other special characters with underscores.
func sanitizeNodeAddr(addr string) string {
	// Replace common special characters
	sanitized := strings.ReplaceAll(addr, ":", "_")
	sanitized = strings.ReplaceAll(sanitized, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "\\", "_")
	return sanitized
}

// startMonitor starts the background node status monitoring goroutine.
func (hc *handoffController) startMonitor() {
	if hc.tire2Client == nil || len(hc.allDataNodes) == 0 {
		hc.l.Info().Msg("node status monitor disabled (no tire2 client or data nodes)")
		return
	}

	// Initialize healthy nodes from tire2 client
	currentHealthy := hc.tire2Client.HealthyNodes()
	for _, node := range currentHealthy {
		hc.healthyNodes[node] = struct{}{}
	}

	hc.monitorWg.Add(2)

	// Goroutine 1: Periodic polling
	go hc.pollNodeStatus()

	// Goroutine 2: Handle status changes
	go hc.handleStatusChanges()

	hc.l.Info().
		Int("dataNodes", len(hc.allDataNodes)).
		Dur("checkInterval", hc.checkInterval).
		Msg("node status monitor started")
}

// pollNodeStatus periodically polls the pub client for healthy nodes.
func (hc *handoffController) pollNodeStatus() {
	defer hc.monitorWg.Done()

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkAndNotifyStatusChanges()
		case <-hc.stopMonitor:
			return
		}
	}
}

// checkAndNotifyStatusChanges compares current vs previous health status.
func (hc *handoffController) checkAndNotifyStatusChanges() {
	if hc.tire2Client == nil {
		return
	}

	currentHealthy := hc.tire2Client.HealthyNodes()
	currentHealthySet := make(map[string]struct{})
	for _, node := range currentHealthy {
		currentHealthySet[node] = struct{}{}
	}

	hc.mu.Lock()
	previousHealthy := hc.healthyNodes
	hc.healthyNodes = currentHealthySet
	hc.mu.Unlock()

	// Detect nodes that came online (in current but not in previous)
	for node := range currentHealthySet {
		if _, wasHealthy := previousHealthy[node]; !wasHealthy {
			select {
			case hc.statusChangeChan <- nodeStatusChange{nodeName: node, isOnline: true}:
				hc.l.Info().Str("node", node).Msg("detected node coming online")
			default:
				hc.l.Warn().Str("node", node).Msg("status change channel full")
			}
		}
	}

	// Detect nodes that went offline (in previous but not in current)
	for node := range previousHealthy {
		if _, isHealthy := currentHealthySet[node]; !isHealthy {
			select {
			case hc.statusChangeChan <- nodeStatusChange{nodeName: node, isOnline: false}:
				hc.l.Info().Str("node", node).Msg("detected node going offline")
			default:
				hc.l.Warn().Str("node", node).Msg("status change channel full")
			}
		}
	}
}

// handleStatusChanges processes node status changes.
func (hc *handoffController) handleStatusChanges() {
	defer hc.monitorWg.Done()

	for {
		select {
		case change := <-hc.statusChangeChan:
			if change.isOnline {
				hc.onNodeOnline(change.nodeName)
			} else {
				hc.onNodeOffline(change.nodeName)
			}
		case <-hc.stopMonitor:
			return
		}
	}
}

// onNodeOnline handles a node coming online.
func (hc *handoffController) onNodeOnline(nodeName string) {
	hc.mu.RLock()
	_, hasQueue := hc.nodeQueues[nodeName]
	hc.mu.RUnlock()

	if !hasQueue {
		hc.l.Debug().Str("node", nodeName).Msg("node online but no handoff queue")
		return
	}

	// Trigger replay for this node
	select {
	case hc.replayTriggerChan <- nodeName:
		hc.l.Info().Str("node", nodeName).Msg("triggered replay for recovered node")
	default:
		hc.l.Warn().Str("node", nodeName).Msg("replay trigger channel full")
	}
}

// onNodeOffline handles a node going offline.
func (hc *handoffController) onNodeOffline(nodeName string) {
	hc.l.Info().Str("node", nodeName).Msg("node went offline")
	// No immediate action needed - syncer will detect send failures
}

// isNodeHealthy checks if a specific node is currently healthy.
func (hc *handoffController) isNodeHealthy(nodeName string) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	_, healthy := hc.healthyNodes[nodeName]
	return healthy
}

// startReplayWorker starts the background replay worker goroutine.
func (hc *handoffController) startReplayWorker() {
	if hc.tire2Client == nil {
		hc.l.Info().Msg("replay worker disabled (no tire2 client)")
		return
	}

	hc.replayWg.Add(1)
	go hc.replayWorkerLoop()

	hc.l.Info().
		Int("batchSize", hc.replayBatchSize).
		Dur("pollInterval", hc.replayPollInterval).
		Msg("replay worker started")
}

// replayWorkerLoop is the main replay worker loop.
func (hc *handoffController) replayWorkerLoop() {
	defer hc.replayWg.Done()

	ticker := time.NewTicker(hc.replayPollInterval)
	defer ticker.Stop()

	// Track nodes that have been triggered for replay
	triggeredNodes := make(map[string]struct{})

	for {
		select {
		case nodeAddr := <-hc.replayTriggerChan:
			// Node came online, mark for immediate processing
			triggeredNodes[nodeAddr] = struct{}{}
			hc.l.Debug().Str("node", nodeAddr).Msg("node marked for replay")

		case <-ticker.C:
			// Periodic check for work
			nodesWithWork := hc.getNodesWithPendingParts()

			// Process triggered nodes first, then round-robin through all nodes with work
			nodesToProcess := make([]string, 0, len(triggeredNodes)+len(nodesWithWork))
			for node := range triggeredNodes {
				nodesToProcess = append(nodesToProcess, node)
			}
			for _, node := range nodesWithWork {
				if _, alreadyTriggered := triggeredNodes[node]; !alreadyTriggered {
					nodesToProcess = append(nodesToProcess, node)
				}
			}

			// Process each node with pending parts (round-robin)
			for _, nodeAddr := range nodesToProcess {
				select {
				case <-hc.replayStopChan:
					return
				default:
				}

				// Check if node is healthy
				if !hc.isNodeHealthy(nodeAddr) {
					continue
				}

				// Process a batch for this node
				processed, err := hc.replayBatchForNode(nodeAddr, hc.replayBatchSize)
				if err != nil {
					hc.l.Warn().Err(err).Str("node", nodeAddr).Msg("replay batch failed")
				}

				// If we processed parts successfully, remove from triggered list
				if processed > 0 {
					delete(triggeredNodes, nodeAddr)
				}

				// If node has no more pending parts, remove from triggered list
				pending, _ := hc.listPendingForNode(nodeAddr)
				if len(pending) == 0 {
					delete(triggeredNodes, nodeAddr)
				}
			}

		case <-hc.replayStopChan:
			return
		}
	}
}

// replayBatchForNode processes a batch of parts for a specific node.
// Returns the number of parts successfully replayed and any error.
func (hc *handoffController) replayBatchForNode(nodeAddr string, maxParts int) (int, error) {
	// Get pending parts for this node
	pending, err := hc.listPendingForNode(nodeAddr)
	if err != nil {
		return 0, fmt.Errorf("failed to list pending parts: %w", err)
	}

	if len(pending) == 0 {
		return 0, nil
	}

	// Limit to batch size
	if len(pending) > maxParts {
		pending = pending[:maxParts]
	}

	successCount := 0
	ctx := context.Background()

	for _, ptp := range pending {
		// Check if already in-flight
		if hc.isInFlight(nodeAddr, ptp.PartID) {
			hc.l.Debug().
				Str("node", nodeAddr).
				Uint64("partID", ptp.PartID).
				Str("partType", ptp.PartType).
				Msg("skipping in-flight part")
			continue
		}

		// Mark as in-flight
		hc.markInFlight(nodeAddr, ptp.PartID, true)

		// Read part from handoff queue
		streamingPart, release, err := hc.readPartFromHandoff(nodeAddr, ptp.PartID, ptp.PartType)
		if err != nil {
			hc.l.Error().Err(err).
				Str("node", nodeAddr).
				Uint64("partID", ptp.PartID).
				Str("partType", ptp.PartType).
				Msg("failed to read part from handoff")
			hc.markInFlight(nodeAddr, ptp.PartID, false)
			continue
		}

		// Send part to node
		err = hc.sendPartToNode(ctx, nodeAddr, streamingPart)
		release()
		if err != nil {
			hc.l.Warn().Err(err).
				Str("node", nodeAddr).
				Uint64("partID", ptp.PartID).
				Str("partType", ptp.PartType).
				Msg("failed to send part during replay")
			hc.markInFlight(nodeAddr, ptp.PartID, false)
			continue
		}

		// Mark as complete
		if err := hc.completeSend(nodeAddr, ptp.PartID, ptp.PartType); err != nil {
			hc.l.Warn().Err(err).
				Str("node", nodeAddr).
				Uint64("partID", ptp.PartID).
				Str("partType", ptp.PartType).
				Msg("failed to mark part as complete")
		}

		// Remove from in-flight
		hc.markInFlight(nodeAddr, ptp.PartID, false)

		successCount++

		hc.l.Info().
			Str("node", nodeAddr).
			Uint64("partID", ptp.PartID).
			Str("partType", ptp.PartType).
			Msg("successfully replayed part")
	}

	return successCount, nil
}

// markInFlight marks a part as being sent (or removes the mark).
func (hc *handoffController) markInFlight(nodeAddr string, partID uint64, inFlight bool) {
	hc.inFlightMu.Lock()
	defer hc.inFlightMu.Unlock()

	if inFlight {
		// Add to in-flight set
		if hc.inFlightSends[nodeAddr] == nil {
			hc.inFlightSends[nodeAddr] = make(map[uint64]struct{})
		}
		hc.inFlightSends[nodeAddr][partID] = struct{}{}
	} else {
		// Remove from in-flight set
		if nodeSet, exists := hc.inFlightSends[nodeAddr]; exists {
			delete(nodeSet, partID)
			if len(nodeSet) == 0 {
				delete(hc.inFlightSends, nodeAddr)
			}
		}
	}
}

// isInFlight checks if a part is currently being sent to a node.
func (hc *handoffController) isInFlight(nodeAddr string, partID uint64) bool {
	hc.inFlightMu.RLock()
	defer hc.inFlightMu.RUnlock()

	if nodeSet, exists := hc.inFlightSends[nodeAddr]; exists {
		_, inFlight := nodeSet[partID]
		return inFlight
	}
	return false
}

// getNodesWithPendingParts returns all node addresses that have pending parts in their queues.
func (hc *handoffController) getNodesWithPendingParts() []string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	var nodes []string
	for nodeAddr, queue := range hc.nodeQueues {
		pending, _ := queue.listPending()
		if len(pending) > 0 {
			nodes = append(nodes, nodeAddr)
		}
	}

	return nodes
}

// readPartFromHandoff reads a part from the handoff queue and prepares it for sending.
func (hc *handoffController) readPartFromHandoff(nodeAddr string, partID uint64, partType string) (*queue.StreamingPartData, func(), error) {
	// Get the path to the hardlinked part
	partPath := hc.getPartPath(nodeAddr, partID, partType)
	if partPath == "" {
		return nil, func() {}, fmt.Errorf("part not found in handoff queue")
	}

	// Get the metadata for this part
	meta, err := hc.getPartMetadata(nodeAddr, partID, partType)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to get part metadata: %w", err)
	}

	// Read files directly from the filesystem (both core and sidx parts)
	// The handoff storage uses nested structure: <nodeRoot>/<partID>/<partType>/
	entries := hc.fileSystem.ReadDir(partPath)
	var files []queue.FileInfo
	var buffers []*bytes.Buffer

	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		streamName, include := mapStreamingFileName(partType, entry.Name())
		if !include {
			continue
		}

		filePath := filepath.Join(partPath, entry.Name())
		data, err := hc.fileSystem.Read(filePath)
		if err != nil {
			hc.l.Warn().Err(err).Str("file", entry.Name()).Msg("failed to read file")
			continue
		}

		// Create a buffer and use its sequential reader
		buf := bigValuePool.Generate()
		buf.Buf = append(buf.Buf[:0], data...)
		buffers = append(buffers, buf)

		files = append(files, queue.FileInfo{
			Name:   streamName,
			Reader: buf.SequentialRead(),
		})
	}

	if len(files) == 0 {
		for _, buf := range buffers {
			bigValuePool.Release(buf)
		}
		return nil, func() {}, fmt.Errorf("no files found in part directory")
	}

	// Create streaming part data
	streamingPart := &queue.StreamingPartData{
		ID:       partID,
		Group:    meta.Group,
		ShardID:  meta.ShardID,
		Topic:    data.TopicTracePartSync.String(),
		Files:    files,
		PartType: partType,
	}

	// For core parts, read additional metadata from metadata.json if present
	if partType == PartTypeCore {
		metadataPath := filepath.Join(partPath, metadataFilename)
		if metadataBytes, err := hc.fileSystem.Read(metadataPath); err == nil {
			var pm partMetadata
			if err := json.Unmarshal(metadataBytes, &pm); err == nil {
				streamingPart.CompressedSizeBytes = pm.CompressedSizeBytes
				streamingPart.UncompressedSizeBytes = pm.UncompressedSpanSizeBytes
				streamingPart.TotalCount = pm.TotalCount
				streamingPart.BlocksCount = pm.BlocksCount
				streamingPart.MinTimestamp = pm.MinTimestamp
				streamingPart.MaxTimestamp = pm.MaxTimestamp
			}
		}
	}

	release := func() {
		for _, buf := range buffers {
			bigValuePool.Release(buf)
		}
	}

	return streamingPart, release, nil
}

func mapStreamingFileName(partType, fileName string) (string, bool) {
	if partType == PartTypeCore {
		switch fileName {
		case primaryFilename:
			return tracePrimaryName, true
		case spansFilename:
			return traceSpansName, true
		case metaFilename:
			return traceMetaName, true
		case traceIDFilterFilename, tagTypeFilename:
			return fileName, true
		case metadataFilename:
			return "", false
		}

		if strings.HasSuffix(fileName, tagsFilenameExt) {
			tagName := strings.TrimSuffix(fileName, tagsFilenameExt)
			return traceTagsPrefix + tagName, true
		}
		if strings.HasSuffix(fileName, tagsMetadataFilenameExt) {
			tagName := strings.TrimSuffix(fileName, tagsMetadataFilenameExt)
			return traceTagMetadataPrefix + tagName, true
		}

		return "", false
	}

	switch fileName {
	case sidx.SidxPrimaryName + ".bin":
		return sidx.SidxPrimaryName, true
	case sidx.SidxDataName + ".bin":
		return sidx.SidxDataName, true
	case sidx.SidxKeysName + ".bin":
		return sidx.SidxKeysName, true
	case sidx.SidxMetaName + ".bin":
		return sidx.SidxMetaName, true
	case "manifest.json":
		return "", false
	}

	if strings.HasSuffix(fileName, ".td") {
		tagName := strings.TrimSuffix(fileName, ".td")
		return sidx.TagDataPrefix + tagName, true
	}
	if strings.HasSuffix(fileName, ".tm") {
		tagName := strings.TrimSuffix(fileName, ".tm")
		return sidx.TagMetadataPrefix + tagName, true
	}
	if strings.HasSuffix(fileName, ".tf") {
		tagName := strings.TrimSuffix(fileName, ".tf")
		return sidx.TagFilterPrefix + tagName, true
	}

	return "", false
}

// sendPartToNode sends a single part to a node using ChunkedSyncClient.
func (hc *handoffController) sendPartToNode(ctx context.Context, nodeAddr string, streamingPart *queue.StreamingPartData) error {
	// Create chunked sync client
	chunkedClient, err := hc.tire2Client.NewChunkedSyncClient(nodeAddr, 1024*1024)
	if err != nil {
		return fmt.Errorf("failed to create chunked sync client: %w", err)
	}
	defer chunkedClient.Close()

	// Send the part
	result, err := chunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{*streamingPart})
	if err != nil {
		return fmt.Errorf("failed to sync streaming part: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("sync failed: %s", result.ErrorMessage)
	}

	hc.l.Debug().
		Str("node", nodeAddr).
		Uint64("partID", streamingPart.ID).
		Str("partType", streamingPart.PartType).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Msg("part sent successfully during replay")

	return nil
}
