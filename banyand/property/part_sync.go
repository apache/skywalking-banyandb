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

package property

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// externalPartTempDirName is the directory, next to the property data directory, where
// received shard files are staged until their documents are repaired into the store.
const externalPartTempDirName = "external-part-temp"

var _ queue.ChunkedSyncHandler = (*partSyncHandler)(nil)

// partSyncHandler receives the native index files of one property shard and repairs every
// stored document into the local shard. The files cannot be introduced as an external
// segment: segment introduction deduplicates by document identifier only, so it would revive
// deleted properties or drop deletions, and a bare segment loses the deletion masks of its
// snapshot manifest. Repair merges by revision and lets tombstones win instead.
type partSyncHandler struct {
	db        db.Database
	checkDisk func() *common.Error
	tempRoot  string
}

// CreatePartHandler stages the part in a fresh directory, so concurrent streams of the
// same shard never share files.
func (h *partSyncHandler) CreatePartHandler(ctx *queue.ChunkedSyncPartContext) (queue.PartHandler, error) {
	if err := h.checkDisk(); err != nil {
		return nil, fmt.Errorf("reject property part %d of group %s: %w", ctx.ID, ctx.Group, err)
	}
	if err := os.MkdirAll(h.tempRoot, storage.DirPerm); err != nil {
		return nil, fmt.Errorf("create property part staging root: %w", err)
	}
	dir, err := os.MkdirTemp(h.tempRoot, "part-")
	if err != nil {
		return nil, fmt.Errorf("create property part staging directory: %w", err)
	}
	return &partSyncContext{
		ctx:     ctx.RetrieveContext(),
		handler: h,
		dir:     dir,
		group:   ctx.Group,
		shardID: ctx.ShardID,
	}, nil
}

// HandleFileChunk appends the chunk to the staged file it belongs to.
func (h *partSyncHandler) HandleFileChunk(ctx *queue.ChunkedSyncPartContext, chunk []byte) error {
	partCtx, ok := ctx.Handler.(*partSyncContext)
	if !ok {
		return fmt.Errorf("unexpected property part handler %T", ctx.Handler)
	}
	return partCtx.write(ctx.FileName, chunk)
}

// partSyncContext stages one part. FinishSync and Close both remove the staging directory:
// a stream that breaks off reaches only Close, and a session or part switch may call
// FinishSync on an incomplete part, which the strict open then rejects before any repair.
type partSyncContext struct {
	ctx      context.Context
	handler  *partSyncHandler
	file     *os.File
	dir      string
	group    string
	fileName string
	shardID  uint32
}

func (p *partSyncContext) write(name string, chunk []byte) error {
	if name != p.fileName {
		if !filepath.IsLocal(name) || filepath.Base(name) != name {
			return fmt.Errorf("invalid property part file name %q", name)
		}
		if err := p.closeFile(); err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(p.dir, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, storage.FilePerm)
		if err != nil {
			return fmt.Errorf("open staged property file %q: %w", name, err)
		}
		p.file, p.fileName = file, name
	}
	if _, err := p.file.Write(chunk); err != nil {
		return fmt.Errorf("write staged property file %q: %w", name, err)
	}
	return nil
}

// NewPartType rejects a second part type: a property part is a single index directory.
func (p *partSyncContext) NewPartType(ctx *queue.ChunkedSyncPartContext) error {
	return fmt.Errorf("property part sync does not support part type %q", ctx.PartType)
}

// FinishSync repairs every document of the staged index into the shard. The strict
// read-only open validates the newest staged generation as a whole before any document is
// visited and never falls back to an older one, so a part missing files or holding a
// truncated one repairs nothing.
func (p *partSyncContext) FinishSync() (err error) {
	defer func() {
		err = multierr.Append(err, p.Close())
	}()
	if err = p.closeFile(); err != nil {
		return err
	}
	// Repair rewrites every staged document into the store, so the disk is checked again.
	if diskErr := p.handler.checkDisk(); diskErr != nil {
		return fmt.Errorf("reject repairing property part of group %s: %w", p.group, diskErr)
	}
	generation, err := native.OpenReadOnlyGenerationStrict(p.dir)
	if err != nil {
		return fmt.Errorf("open staged property index: %w", err)
	}
	defer func() {
		err = multierr.Append(err, generation.Close())
	}()
	return generation.VisitLiveDocuments(p.ctx, p.repair)
}

func (p *partSyncContext) repair(doc native.StoredDocument) error {
	property, deleteTime, err := db.DecodeStoredDocument(doc)
	if err != nil {
		return fmt.Errorf("decode staged property document: %w", err)
	}
	name, id := property.GetMetadata().GetName(), property.GetId()
	if group := property.GetMetadata().GetGroup(); group != p.group {
		return fmt.Errorf("property %s/%s belongs to group %q, not the synced group %q", name, id, group, p.group)
	}
	if err = p.handler.db.Repair(p.ctx, db.GetPropertyID(property), uint64(p.shardID), property, deleteTime); err != nil {
		return fmt.Errorf("repair property %s/%s: %w", name, id, err)
	}
	return nil
}

// Close discards the staged part. It is safe to call more than once.
func (p *partSyncContext) Close() error {
	err := p.closeFile()
	if p.dir != "" {
		err = multierr.Append(err, os.RemoveAll(p.dir))
		p.dir = ""
	}
	return err
}

func (p *partSyncContext) closeFile() error {
	if p.file == nil {
		return nil
	}
	name := p.fileName
	err := p.file.Close()
	p.file, p.fileName = nil, ""
	if err != nil {
		return fmt.Errorf("close staged property file %q: %w", name, err)
	}
	return nil
}
