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
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	streamReplayBatchTimeout = 30 * time.Second
)

type cachedStreamSchema struct {
	schema        *databasev1.Stream
	entityLocator partition.Locator
}

// streamRowReplayer replays a group's stream parts row-by-row.
type streamRowReplayer struct {
	selector       node.Selector
	sender         *batchSender
	metadata       metadata.Repo
	fs             fs.FileSystem
	logger         *logger.Logger
	schemaCache    map[string]*cachedStreamSchema
	irResolver     *dump.IndexResolver
	counter        *uint64
	archiver       *orphanArchiver
	group          string
	irPath         string
	srcStage       string
	schemaCacheMu  sync.Mutex
	irMu           sync.Mutex
	targetShardNum uint32
}

func newStreamRowReplayer(
	group string, targetShardNum uint32,
	selector node.Selector, client queue.Client,
	md metadata.Repo, fileSystem fs.FileSystem,
	l *logger.Logger, counter *uint64, orphanCfg orphanConfig, srcStage string,
) *streamRowReplayer {
	return &streamRowReplayer{
		group:          group,
		targetShardNum: targetShardNum,
		selector:       selector,
		sender:         newBatchSender(client, data.TopicStreamWrite, rowReplayMaxBatchRows, int(rowReplayMaxBatchBytes), streamReplayBatchTimeout),
		archiver:       newOrphanArchiver(orphanCfg, group, catalogStream, l),
		metadata:       md,
		fs:             fileSystem,
		logger:         l,
		schemaCache:    make(map[string]*cachedStreamSchema),
		counter:        counter,
		srcStage:       srcStage,
	}
}

// Close drains any outstanding batch confirmations, closes the publisher and
// releases cached IndexResolver handles.
func (r *streamRowReplayer) Close() (map[string]*common.Error, error) {
	cee, closeErr := r.sender.close()
	closeIndexResolver(&r.irMu, &r.irResolver, &r.irPath)
	return cee, closeErr
}

// loadIndexResolver lazily opens (and keeps resident) the IndexResolver for the
// current source segment via the shared helper. Stream rows carry no indexed
// values to decode, so the rule-to-tag map is nil.
func (r *streamRowReplayer) loadIndexResolver(segmentPath string) (*dump.IndexResolver, error) {
	return reloadIndexResolver(&r.irMu, &r.irResolver, &r.irPath, segmentPath, nil)
}

func (r *streamRowReplayer) loadSchema(ctx context.Context, streamName string) (*cachedStreamSchema, error) {
	r.schemaCacheMu.Lock()
	defer r.schemaCacheMu.Unlock()
	if c, ok := r.schemaCache[streamName]; ok {
		return c, nil
	}
	s, err := r.metadata.StreamRegistry().GetStream(ctx, &commonv1.Metadata{Group: r.group, Name: streamName})
	if err != nil {
		// A genuine registry not-found means the stream was deleted from the
		// registry (orphan); any other error (network, closed registry, ...) stays
		// fatal so a transient failure is never mistaken for a droppable orphan.
		if errors.Is(err, schema.ErrGRPCResourceNotFound) {
			return nil, fmt.Errorf("%w: stream %s/%s", errOrphanSchema, r.group, streamName)
		}
		return nil, fmt.Errorf("load stream schema %s/%s: %w", r.group, streamName, err)
	}
	c := &cachedStreamSchema{
		schema:        s,
		entityLocator: partition.NewEntityLocator(s.TagFamilies, s.Entity, s.GetMetadata().GetModRevision()),
	}
	r.schemaCache[streamName] = c
	return c, nil
}

// replayPart opens a source part and replays its rows through the sender. It
// returns the rows published, the rows dropped because their series could not be
// resolved (skipped — a series-index gap, mirroring measure: the source segment
// must be retained), and the rows dropped because their schema was deleted from
// the registry (orphanSkipped). Both skip counts are this part's delta from the
// replayer-wide sender tallies. A non-nil error means some rows were not durably
// delivered. Returns the same partReplayResult shape as the measure replayer.
func (r *streamRowReplayer) replayPart(ctx context.Context, partPath string) (partReplayResult, error) {
	partID, shardPath, segmentPath, parseErr := parseReplayPartPath(partPath)
	if parseErr != nil {
		return partReplayResult{}, parseErr
	}

	reader, openErr := dumpstream.OpenPart(partID, shardPath, r.fs)
	if openErr != nil {
		return partReplayResult{}, fmt.Errorf("open stream part %s: %w", partPath, openErr)
	}
	defer reader.Close()

	ir, irErr := r.loadIndexResolver(segmentPath)
	if irErr != nil {
		return partReplayResult{}, fmt.Errorf("load stream index resolver for segment %s: %w", segmentPath, irErr)
	}
	reader.SetIndexResolver(ir)

	loc := sourceLoc{
		Stage:   r.srcStage,
		Segment: segmentSuffixFromPath(segmentPath),
		Shard:   shardFromPath(shardPath),
		Part:    fmt.Sprintf("%016x", partID),
	}

	it := reader.Iterator()
	defer it.Close()
	// runPart opens the part's archive writer and commits/aborts its manifest; the
	// orphan rows are appended to that writer (captured by the emit closure) inside replay.
	var res partReplayResult
	runErr := r.archiver.runPart(loc, func(pw *orphanPartWriter) error {
		var replayErr error
		res, replayErr = r.sender.replay(ctx, r.logger, r.group, partPath, r.counter, it,
			func() error { return r.publishRow(ctx, pw, it.Row()) })
		return replayErr
	})
	return res, runErr
}

// buildWriteRequest reconstructs the WriteRequest + InternalWriteRequest pair
// from a resolved subject + raw stream Row. The subject and its entity TagValues
// are decoded by the caller so an orphan schema can be archived before the build.
// Separated from publishRow for testability.
func (r *streamRowReplayer) buildWriteRequest(
	cached *cachedStreamSchema, subject string, evList pbv1.EntityValues, row dumpstream.Row,
) (*streamv1.WriteRequest, *streamv1.InternalWriteRequest, error) {
	tagFamilies := buildStreamTagFamilies(cached.schema.TagFamilies, cached.schema.Entity, row, evList)
	wr := &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: r.group, Name: subject},
		Element: &streamv1.ElementValue{
			ElementId:   base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(row.ElementID)),
			Timestamp:   timestamppb.New(time.Unix(0, row.Timestamp)),
			TagFamilies: tagFamilies,
		},
		MessageId: uint64(time.Now().UnixNano()),
	}
	tagValues, shardID, err := partition.ApplyLocators(subject, tagFamilies,
		cached.entityLocator, nil, r.targetShardNum)
	if err != nil {
		return nil, nil, fmt.Errorf("route %s: %w", subject, err)
	}
	iwr := &streamv1.InternalWriteRequest{
		ShardId:      uint32(shardID),
		EntityValues: tagValues[1:].Encode(),
		Request:      wr,
		RawElementId: row.ElementID,
	}
	return wr, iwr, nil
}

func (r *streamRowReplayer) publishRow(ctx context.Context, pw *orphanPartWriter, row dumpstream.Row) error {
	subject, evList, err := decodeSeriesEntityValues(row.EntityValues)
	if err != nil {
		return fmt.Errorf("decode entity values (seriesID=%d): %w", row.SeriesID, err)
	}
	cached, err := r.loadSchema(ctx, subject)
	if err != nil {
		if errors.Is(err, errOrphanSchema) {
			// Deleted schema: archive (or discard) this row, then skip it. The source
			// segment is still deleted by the visitor (unlike a sidx-gap skip). A
			// failed archive write under the archive policy is FATAL — returning it
			// aborts the part so the source is retained and resume retries, never
			// silently dropping the orphan row.
			if archiveErr := r.archiveOrphanRow(pw, subject, evList, row); archiveErr != nil {
				return archiveErr
			}
			loc := pw.loc
			partLabel := fmt.Sprintf("seg-%s/shard-%d/part-%s", loc.Segment, loc.Shard, loc.Part)
			return newOrphanSkip(partLabel, uint64(row.SeriesID), subject)
		}
		return err
	}
	wr, iwr, err := r.buildWriteRequest(cached, subject, evList, row)
	if err != nil {
		return err
	}
	return r.sender.routeAndEnqueue(ctx, r.selector, r.group, wr.Metadata.Name, iwr.ShardId, iwr)
}

// archiveOrphanRow writes one orphan stream row to the part archive (a no-op
// write when policy is discard, which still tallies via the writer). It returns
// a non-nil error when the archive write fails so the caller can treat it as
// fatal (under the archive policy) and retain the source segment.
func (r *streamRowReplayer) archiveOrphanRow(pw *orphanPartWriter, subject string, evList pbv1.EntityValues, row dumpstream.Row) error {
	rec := &archiveRecord{
		Group: r.group, Catalog: catalogStream, Measure: subject,
		Source:    pw.loc,
		SeriesID:  uint64(row.SeriesID),
		Entity:    orphanEntityStrings(evList, row.EntityValues),
		Timestamp: time.Unix(0, row.Timestamp).UTC().Format(time.RFC3339Nano),
		TimeNanos: row.Timestamp,
		ElementID: base64.StdEncoding.EncodeToString(convert.Uint64ToBytes(row.ElementID)),
	}
	if len(row.Tags) > 0 {
		rec.Tags = make(map[string]typedValue, len(row.Tags))
		for name, raw := range row.Tags {
			rec.Tags[name] = valueWithType(dump.DecodeTagValue(row.TagTypes[name], raw, nil))
		}
	}
	if err := pw.appendRow(rec); err != nil {
		return fmt.Errorf("archive orphan stream %s row: %w", subject, err)
	}
	return nil
}
