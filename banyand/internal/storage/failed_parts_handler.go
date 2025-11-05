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

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	// DefaultInitialRetryDelay is the initial delay before the first retry.
	DefaultInitialRetryDelay = 1 * time.Second
	// DefaultMaxRetries is the maximum number of retry attempts.
	DefaultMaxRetries = 3
	// DefaultBackoffMultiplier is the multiplier for exponential backoff.
	DefaultBackoffMultiplier = 2
	// FailedPartsDirName is the name of the directory for failed parts.
	FailedPartsDirName = "failed-parts"
)

// FailedPartsHandler handles retry logic and filesystem fallback for failed parts.
type FailedPartsHandler struct {
	fileSystem        fs.FileSystem
	l                 *logger.Logger
	root              string
	failedPartsDir    string
	initialRetryDelay time.Duration
	maxRetries        int
	backoffMultiplier int
}

// PartInfo contains information needed to retry or copy a failed part.
type PartInfo struct {
	SourcePath string
	PartType   string
	PartID     uint64
}

// NewFailedPartsHandler creates a new handler for failed parts.
func NewFailedPartsHandler(fileSystem fs.FileSystem, root string, l *logger.Logger) *FailedPartsHandler {
	failedPartsDir := filepath.Join(root, FailedPartsDirName)
	fileSystem.MkdirIfNotExist(failedPartsDir, DirPerm)

	return &FailedPartsHandler{
		fileSystem:        fileSystem,
		root:              root,
		failedPartsDir:    failedPartsDir,
		l:                 l,
		initialRetryDelay: DefaultInitialRetryDelay,
		maxRetries:        DefaultMaxRetries,
		backoffMultiplier: DefaultBackoffMultiplier,
	}
}

// RetryFailedParts attempts to retry failed parts with exponential backoff.
// Returns the list of permanently failed part IDs after all retries are exhausted.
func (h *FailedPartsHandler) RetryFailedParts(
	ctx context.Context,
	failedParts []queue.FailedPart,
	partsInfo map[uint64][]*PartInfo,
	syncFunc func(partIDs []uint64) ([]queue.FailedPart, error),
) ([]uint64, error) {
	if len(failedParts) == 0 {
		return nil, nil
	}

	// Group failed parts by part ID
	failedPartIDs := make(map[uint64]string)
	for _, fp := range failedParts {
		partID, err := strconv.ParseUint(fp.PartID, 10, 64)
		if err != nil {
			h.l.Warn().Err(err).Str("partID", fp.PartID).Msg("failed to parse part ID, skipping")
			continue
		}
		failedPartIDs[partID] = fp.Error
	}

	h.l.Warn().
		Int("count", len(failedPartIDs)).
		Msg("starting retry process for failed parts")

	// Retry with exponential backoff
	var stillFailing []uint64
	for partID, errMsg := range failedPartIDs {
		if err := h.retryPartWithBackoff(ctx, partID, errMsg, syncFunc); err != nil {
			h.l.Error().
				Err(err).
				Uint64("partID", partID).
				Msg("part failed after all retries")
			stillFailing = append(stillFailing, partID)
		}
	}

	// Copy permanently failed parts to failed-parts directory
	var permanentlyFailed []uint64
	for _, partID := range stillFailing {
		partInfoList, exists := partsInfo[partID]
		if !exists || len(partInfoList) == 0 {
			h.l.Warn().Uint64("partID", partID).Msg("no part info found for failed part")
			permanentlyFailed = append(permanentlyFailed, partID)
			continue
		}

		// Copy all parts with this ID (core + all SIDX parts)
		allCopied := true
		for _, partInfo := range partInfoList {
			destSubDir := fmt.Sprintf("%016x_%s", partID, partInfo.PartType)
			if err := h.CopyToFailedPartsDir(partID, partInfo.SourcePath, destSubDir); err != nil {
				h.l.Error().
					Err(err).
					Uint64("partID", partID).
					Str("partType", partInfo.PartType).
					Str("sourcePath", partInfo.SourcePath).
					Msg("failed to copy part to failed-parts directory")
				allCopied = false
			} else {
				h.l.Info().
					Uint64("partID", partID).
					Str("partType", partInfo.PartType).
					Str("destination", filepath.Join(h.failedPartsDir, destSubDir)).
					Msg("successfully copied failed part to failed-parts directory")
			}
		}
		if !allCopied {
			h.l.Warn().Uint64("partID", partID).Msg("some parts failed to copy")
		}
		permanentlyFailed = append(permanentlyFailed, partID)
	}

	return permanentlyFailed, nil
}

// retryPartWithBackoff retries a single part with exponential backoff.
func (h *FailedPartsHandler) retryPartWithBackoff(
	ctx context.Context,
	partID uint64,
	initialError string,
	syncFunc func(partIDs []uint64) ([]queue.FailedPart, error),
) error {
	delay := h.initialRetryDelay

	for attempt := 1; attempt <= h.maxRetries; attempt++ {
		// Wait before retry
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled during retry: %w", ctx.Err())
		case <-time.After(delay):
		}

		h.l.Info().
			Uint64("partID", partID).
			Int("attempt", attempt).
			Int("maxRetries", h.maxRetries).
			Dur("delay", delay).
			Msg("retrying failed part")

		// Attempt to sync just this part
		newFailedParts, err := syncFunc([]uint64{partID})
		if err != nil {
			h.l.Warn().
				Err(err).
				Uint64("partID", partID).
				Int("attempt", attempt).
				Msg("retry attempt failed with error")
			delay *= time.Duration(h.backoffMultiplier)
			continue
		}

		// Check if this part still failed
		partStillFailed := false
		for _, fp := range newFailedParts {
			fpID, _ := strconv.ParseUint(fp.PartID, 10, 64)
			if fpID == partID {
				partStillFailed = true
				h.l.Warn().
					Uint64("partID", partID).
					Int("attempt", attempt).
					Str("error", fp.Error).
					Msg("retry attempt failed")
				break
			}
		}

		if !partStillFailed {
			h.l.Info().
				Uint64("partID", partID).
				Int("attempt", attempt).
				Msg("part successfully synced after retry")
			return nil
		}

		// Exponential backoff for next attempt
		delay *= time.Duration(h.backoffMultiplier)
	}

	return fmt.Errorf("part failed after %d retry attempts, initial error: %s", h.maxRetries, initialError)
}

// CopyToFailedPartsDir copies a part to the failed-parts directory using hard links.
func (h *FailedPartsHandler) CopyToFailedPartsDir(partID uint64, sourcePath string, destSubDir string) error {
	destPath := filepath.Join(h.failedPartsDir, destSubDir)

	// Check if already exists
	entries := h.fileSystem.ReadDir(h.failedPartsDir)
	for _, entry := range entries {
		if entry.Name() == destSubDir {
			h.l.Info().
				Uint64("partID", partID).
				Str("destSubDir", destSubDir).
				Msg("part already exists in failed-parts directory")
			return nil
		}
	}

	h.l.Info().
		Uint64("partID", partID).
		Str("sourcePath", sourcePath).
		Str("destPath", destPath).
		Msg("creating hard links to failed-parts directory")

	// Create hard links from source to destination
	if err := h.fileSystem.CreateHardLink(sourcePath, destPath, nil); err != nil {
		h.l.Error().
			Err(err).
			Uint64("partID", partID).
			Str("sourcePath", sourcePath).
			Str("destPath", destPath).
			Msg("failed to create hard links")
		return fmt.Errorf("failed to create hard links: %w", err)
	}

	h.fileSystem.SyncPath(destPath)

	h.l.Info().
		Uint64("partID", partID).
		Str("destPath", destPath).
		Msg("successfully created hard links to failed-parts directory")

	return nil
}
