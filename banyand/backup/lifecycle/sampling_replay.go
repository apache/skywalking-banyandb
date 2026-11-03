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

package lifecycle

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
)

// Replay uses a continuous bounded sender session across source part boundaries.
func (p *preparedTraceMigration) Replay(ctx context.Context, r *traceRowReplayer, shardPath string) (int, error) {
	if p.checkpoint.Cursor.Part != "" {
		found := false
		for _, part := range p.manifest.Parts {
			found = found || filepath.Base(part) == p.checkpoint.Cursor.Part
		}
		if !found || p.checkpoint.Cursor.Block < 0 || p.checkpoint.Cursor.Row < 0 {
			return 0, fmt.Errorf("sampling checkpoint outside pinned inventory")
		}
	}
	queuedBefore := r.sender.queuedBytes
	defer func() { p.replayedBytes += r.sender.queuedBytes - queuedBefore }()
	if p.checkpoint.Delivered {
		return 0, nil
	}
	pinnedSchema := &databasev1.Trace{}
	if err := proto.Unmarshal(p.manifest.Schema, pinnedSchema); err != nil {
		return 0, err
	}
	r.schema = pinnedSchema
	r.traceName = pinnedSchema.Metadata.Name
	r.targetShardNum = p.manifest.TargetShardNum
	parts := p.manifest.Parts
	for _, part := range parts {
		if filepath.Dir(part) != shardPath {
			return 0, fmt.Errorf("sampling source part outside pinned shard")
		}
	}
	cursor := p.checkpoint.Cursor
	sequence := p.checkpoint.Sequence
	r.sender.beforeFlush = func() (func() error, error) {
		sequence++
		checkpoint := samplingCheckpoint{ManifestDigest: p.manifest.ManifestDigest, Sequence: sequence, Cursor: cursor}
		return func() error {
			if err := saveSamplingCheckpoint(filepath.Join(p.directory, "checkpoint.json"), checkpoint); err != nil {
				return fmt.Errorf("sampling delivery checkpoint: %w", err)
			}
			p.checkpoint = checkpoint
			return nil
		}, nil
	}
	defer func() { r.sender.beforeFlush = nil }()
	rows := 0
	lastFlush := time.Now()
	var failure error
	for _, partPath := range parts {
		part := filepath.Base(partPath)
		if p.checkpoint.Cursor.Part != "" && part < p.checkpoint.Cursor.Part {
			continue
		}
		partRows, partErr := p.replaySourcePart(ctx, r, partPath, &cursor, &lastFlush)
		rows += partRows
		if partErr != nil {
			failure = partErr
			break
		}
	}
	if failure == nil {
		failure = r.sender.flush(ctx)
	}
	if drainErr := r.sender.drain(); failure == nil {
		failure = drainErr
	}
	if failure != nil {
		r.sender.discard()
		return rows, failure
	}
	completed := p.checkpoint
	completed.Cursor = cursor
	completed.Delivered = true
	if checkpointErr := saveSamplingCheckpoint(filepath.Join(p.directory, "checkpoint.json"), completed); checkpointErr != nil {
		return rows, checkpointErr
	}
	p.checkpoint = completed
	return rows, nil
}

func (p *preparedTraceMigration) replaySourcePart(ctx context.Context, r *traceRowReplayer, partPath string,
	cursor *samplingCursor, lastFlush *time.Time,
) (int, error) {
	part := filepath.Base(partPath)
	rows := 0
	var failure error
	partID, sourcePath, _, parseErr := parseReplayPartPath(partPath)
	if parseErr != nil {
		return 0, parseErr
	}
	reader, openErr := dumptrace.OpenPart(partID, sourcePath, r.fs)
	if openErr != nil {
		return 0, openErr
	}
	iterator := reader.Iterator()
	if part == p.checkpoint.Cursor.Part {
		if seekErr := iterator.SeekAfter(dump.Position{BlockIdx: p.checkpoint.Cursor.Block, RowIdx: p.checkpoint.Cursor.Row}); seekErr != nil {
			iterator.Close()
			reader.Close()
			return 0, seekErr
		}
	}
	for iterator.Next() {
		if pollErr := r.sender.pipeline.poll(); pollErr != nil {
			failure = pollErr
			break
		}
		if ctx.Err() != nil {
			failure = ctx.Err()
			break
		}
		row := iterator.Row()
		keep, lookupErr := p.table.lookup(row.TraceID)
		if lookupErr != nil {
			failure = lookupErr
			break
		}
		if time.Since(*lastFlush) >= time.Second {
			if flushErr := r.sender.flush(ctx); flushErr != nil {
				failure = flushErr
				break
			}
			*lastFlush = time.Now()
		}
		position := iterator.Position()
		*cursor = samplingCursor{Part: part, Block: position.BlockIdx, Row: position.RowIdx}

		if !keep {
			continue
		}
		if publishErr := r.publishRow(ctx, row); publishErr != nil {
			failure = publishErr
			break
		}
		rows++
		if time.Since(*lastFlush) >= time.Second {
			if flushErr := r.sender.flush(ctx); flushErr != nil {
				failure = flushErr
				break
			}
			*lastFlush = time.Now()
		}
	}
	if failure == nil {
		failure = iterator.Err()
	}
	iterator.Close()
	reader.Close()
	return rows, failure
}
