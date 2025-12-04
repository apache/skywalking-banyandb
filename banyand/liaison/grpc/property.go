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

package grpc

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	propertypkg "github.com/apache/skywalking-banyandb/banyand/property"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/accesslog"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	sortpkg "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

const defaultQueryTimeout = 10 * time.Second

type propertyServer struct {
	propertyv1.UnimplementedPropertyServiceServer
	*discoveryService
	ingestionAccessLog accesslog.Log
	queryAccessLog     accesslog.Log
	schemaRegistry     metadata.Repo
	pipeline           queue.Client
	nodeRegistry       NodeRegistry
	metrics            *metrics
	repairQueue        *repairQueue
	repairQueueCount   int
}

func (ps *propertyServer) activeIngestionAccessLog(root string, sampled bool) (err error) {
	if ps.ingestionAccessLog, err = accesslog.
		NewFileLog(root, "property-ingest-%s", 10*time.Minute, ps.log, sampled); err != nil {
		return err
	}
	return nil
}

func (ps *propertyServer) activeQueryAccessLog(root string, sampled bool) (err error) {
	if ps.queryAccessLog, err = accesslog.
		NewFileLog(root, "property-query-%s", 10*time.Minute, ps.log, sampled); err != nil {
		return err
	}
	return nil
}

func (ps *propertyServer) Close() error {
	var err error
	if ps.ingestionAccessLog != nil {
		if closeErr := ps.ingestionAccessLog.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if ps.queryAccessLog != nil {
		if closeErr := ps.queryAccessLog.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}

func (ps *propertyServer) validatePropertyRequest(property *propertyv1.Property) error {
	if property.Metadata == nil {
		return schema.BadRequest("metadata", "metadata should not be nil")
	}
	if property.Metadata.Group == "" {
		return schema.BadRequest("metadata.group", "group should not be nil")
	}
	if property.Metadata.Name == "" {
		return schema.BadRequest("metadata.name", "name should not be empty")
	}
	if property.Id == "" {
		return schema.BadRequest("id", "id should not be empty")
	}
	if len(property.Tags) == 0 {
		return schema.BadRequest("tags", "tags should not be empty")
	}
	return nil
}

func (ps *propertyServer) validateGroupForProperty(ctx context.Context, groupName string) (*commonv1.Group, error) {
	group, err := ps.schemaRegistry.GroupRegistry().GetGroup(ctx, groupName)
	if err != nil {
		return nil, errors.Errorf("group %s not found", groupName)
	}
	if group.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
		return nil, errors.Errorf("group %s is not allowed to have properties", groupName)
	}
	if group.ResourceOpts == nil {
		return nil, errors.Errorf("group %s has no resource options", groupName)
	}
	if group.ResourceOpts.ShardNum == 0 {
		return nil, errors.Errorf("group %s has no shard number", groupName)
	}
	return group, nil
}

func (ps *propertyServer) validatePropertyTags(ctx context.Context, property *propertyv1.Property) error {
	propSchema, err := ps.schemaRegistry.PropertyRegistry().GetProperty(ctx, &commonv1.Metadata{
		Group: property.Metadata.Group,
		Name:  property.Metadata.Name,
	})
	if err != nil {
		return err
	}
	if len(propSchema.Tags) < len(property.Tags) {
		return errors.Errorf("property %s tags count mismatch", property.Metadata.Name)
	}
	for _, tag := range property.Tags {
		found := false
		for _, ts := range propSchema.Tags {
			if ts.Name == tag.Key {
				typ := databasev1.TagType(pbv1.MustTagValueToValueType(tag.Value))
				if typ != databasev1.TagType_TAG_TYPE_UNSPECIFIED && ts.Type != typ {
					return errors.Errorf("property %s tag %s type mismatch", property.Metadata.Name, tag.Key)
				}
				found = true
			}
		}
		if !found {
			return errors.Errorf("property %s tag %s not found", property.Metadata.Name, tag.Key)
		}
	}
	return nil
}

func (ps *propertyServer) locateNodeSetForProperty(property *propertyv1.Property, shardID uint32) ([]string, error) {
	copies, ok := ps.groupRepo.copies(property.Metadata.GetGroup())
	if !ok {
		return nil, errors.New("failed to get group copies")
	}

	nodeSet := make(map[string]struct{}, copies)
	var nodeID string
	var err error
	for i := range copies {
		nodeID, err = ps.nodeRegistry.Locate(property.GetMetadata().GetGroup(), property.GetMetadata().GetName(), shardID, i)
		if err != nil {
			return nil, err
		}
		nodeSet[nodeID] = struct{}{}
	}
	nodes := make([]string, 0, len(nodeSet))
	for nodeID := range nodeSet {
		nodes = append(nodes, nodeID)
	}
	return nodes, nil
}

func (ps *propertyServer) Apply(ctx context.Context, req *propertyv1.ApplyRequest) (resp *propertyv1.ApplyResponse, err error) {
	property := req.Property
	if err = ps.validatePropertyRequest(property); err != nil {
		return nil, err
	}
	g := req.Property.Metadata.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "apply")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "apply")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "apply")
		if err != nil {
			ps.metrics.totalErr.Inc(1, g, "property", "apply")
		}
	}()
	if ps.ingestionAccessLog != nil {
		if errAccessLog := ps.ingestionAccessLog.Write(req); errAccessLog != nil {
			ps.log.Error().Err(errAccessLog).Msg("ingestion access log error")
		}
	}
	var group *commonv1.Group
	if group, err = ps.validateGroupForProperty(ctx, g); err != nil {
		return nil, err
	}
	if err = ps.validatePropertyTags(ctx, property); err != nil {
		return nil, err
	}
	nodeProperties, _, _, err := ps.queryProperties(ctx, &propertyv1.QueryRequest{
		Groups: []string{g},
		Name:   property.Metadata.Name,
		Ids:    []string{property.Id},
	})
	if err != nil {
		return nil, err
	}
	prevPropertyWithMetadata, olderProperties := ps.findPrevAndOlderProperties(nodeProperties)
	entity := propertypkg.GetEntity(property)
	shardID, err := partition.ShardID(convert.StringToBytes(entity), group.ResourceOpts.ShardNum)
	if err != nil {
		return nil, err
	}
	nodes, err := ps.locateNodeSetForProperty(property, uint32(shardID))
	if err != nil {
		return nil, err
	}
	var prev *propertyv1.Property
	if prevPropertyWithMetadata != nil && prevPropertyWithMetadata.deletedTime <= 0 {
		prev = prevPropertyWithMetadata.Property
	}
	defer func() {
		// if their no older properties or have error when apply the new property
		// then ignore cleanup the older properties
		if len(olderProperties) == 0 || err != nil {
			return
		}
		var ids [][]byte
		for _, p := range olderProperties {
			ids = append(ids, propertypkg.GetPropertyID(p.Property))
		}
		_ = ps.remove(ids)
	}()
	if req.Strategy == propertyv1.ApplyRequest_STRATEGY_REPLACE {
		return ps.replaceProperty(ctx, start, uint64(shardID), nodes, prev, property)
	}
	return ps.mergeProperty(ctx, start, uint64(shardID), nodes, prev, property)
}

func (ps *propertyServer) findPrevAndOlderProperties(nodeProperties map[string][]*propertyWithMetadata) (*propertyWithMetadata, []*propertyWithMetadata) {
	var prevPropertyWithMetadata *propertyWithMetadata
	var olderProperties []*propertyWithMetadata
	for _, properties := range nodeProperties {
		for _, p := range properties {
			// if the property is not deleted, then added to the older properties list to delete after apply success
			if p.deletedTime <= 0 {
				olderProperties = append(olderProperties, p)
			}
			// update the prov property
			if prevPropertyWithMetadata == nil || p.Metadata.ModRevision > prevPropertyWithMetadata.Metadata.ModRevision {
				prevPropertyWithMetadata = p
			}
		}
	}
	return prevPropertyWithMetadata, olderProperties
}

func (ps *propertyServer) mergeProperty(ctx context.Context, now time.Time, shardID uint64, nodes []string,
	prev, cur *propertyv1.Property,
) (*propertyv1.ApplyResponse, error) {
	if prev == nil {
		return ps.replaceProperty(ctx, now, shardID, nodes, prev, cur)
	}
	tagCount, err := tagLen(prev)
	if err != nil {
		return nil, err
	}
	tags := make([]*modelv1.Tag, 0)
	for i := 0; i < int(tagCount); i++ {
		t := prev.Tags[i]
		tagExisted := false
		for _, et := range cur.Tags {
			if et.Key == t.Key {
				tagExisted = true
				break
			}
		}
		if !tagExisted {
			tags = append(tags, t)
		}
	}
	cur.Tags = append(cur.Tags, tags...)
	return ps.replaceProperty(ctx, now, shardID, nodes, prev, cur)
}

func tagLen(property *propertyv1.Property) (uint32, error) {
	tagsCount := len(property.Tags)
	if tagsCount < 0 || uint64(tagsCount) > math.MaxUint32 {
		return 0, errors.New("integer overflow: tags count exceeds uint32 range")
	}
	tagsNum := uint32(tagsCount)
	return tagsNum, nil
}

func (ps *propertyServer) replaceProperty(ctx context.Context, now time.Time, shardID uint64, nodes []string,
	prev, cur *propertyv1.Property,
) (*propertyv1.ApplyResponse, error) {
	ns := now.UnixNano()
	if prev != nil {
		cur.Metadata.CreateRevision = prev.Metadata.CreateRevision
	} else {
		cur.Metadata.CreateRevision = ns
	}
	cur.Metadata.ModRevision = ns
	cur.UpdatedAt = timestamppb.New(now)
	req := &propertyv1.InternalUpdateRequest{
		ShardId:  shardID,
		Id:       propertypkg.GetPropertyID(cur),
		Property: cur,
	}
	var successCount int
	futures := make([]bus.Future, 0, len(nodes))
	for _, node := range nodes {
		f, err := ps.pipeline.Publish(ctx, data.TopicPropertyUpdate,
			bus.NewMessageWithNode(bus.MessageID(time.Now().Unix()), node, req))
		if err != nil {
			ps.log.Debug().Err(err).Str("node", node).Msg("failed to publish property update")
			continue
		}
		successCount++
		futures = append(futures, f)
	}
	if successCount == 0 {
		return nil, fmt.Errorf("failed to publish property update to any node")
	}
	// Wait for all futures to complete, and which should last have one success
	haveSuccess := false
	var lastestError error
	for _, f := range futures {
		_, err := f.Get()
		if err == nil {
			haveSuccess = true
		} else {
			lastestError = multierr.Append(lastestError, err)
		}
	}
	if !haveSuccess {
		if lastestError != nil {
			return nil, lastestError
		}
		return nil, errors.New("failed to apply property, no replicas success")
	}

	return &propertyv1.ApplyResponse{
		Created: prev == nil,
		TagsNum: uint32(len(cur.Tags)),
	}, nil
}

func (ps *propertyServer) Delete(ctx context.Context, req *propertyv1.DeleteRequest) (resp *propertyv1.DeleteResponse, err error) {
	if req.Group == "" {
		return nil, schema.BadRequest("group", "group should not be nil")
	}
	if req.Name == "" {
		return nil, schema.BadRequest("name", "name should not be nil")
	}
	g := req.Group
	ps.metrics.totalStarted.Inc(1, g, "property", "delete")
	start := time.Now()
	defer func() {
		ps.metrics.totalFinished.Inc(1, g, "property", "delete")
		ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), g, "property", "delete")
		if err != nil {
			ps.metrics.totalErr.Inc(1, g, "property", "delete")
		}
	}()
	if ps.ingestionAccessLog != nil {
		if errAccessLog := ps.ingestionAccessLog.Write(req); errAccessLog != nil {
			ps.log.Error().Err(errAccessLog).Msg("ingestion access log error")
		}
	}
	qReq := &propertyv1.QueryRequest{
		Groups: []string{g},
		Name:   req.Name,
	}
	if len(req.Id) > 0 {
		qReq.Ids = []string{req.Id}
	}
	nodeProperties, _, _, err := ps.queryProperties(ctx, qReq)
	if err != nil {
		return nil, err
	}
	if len(nodeProperties) == 0 {
		return &propertyv1.DeleteResponse{Deleted: false}, nil
	}
	var ids [][]byte
	for _, properties := range nodeProperties {
		for _, p := range properties {
			// if the property already delete, then ignore execute twice
			if p.deletedTime > 0 {
				continue
			}
			ids = append(ids, propertypkg.GetPropertyID(p.Property))
		}
	}
	if err := ps.remove(ids); err != nil {
		return nil, err
	}
	return &propertyv1.DeleteResponse{Deleted: true}, nil
}

func (ps *propertyServer) Query(ctx context.Context, req *propertyv1.QueryRequest) (resp *propertyv1.QueryResponse, err error) {
	ps.metrics.totalStarted.Inc(1, "", "property", "query")
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		ps.metrics.totalFinished.Inc(1, "", "property", "query")
		ps.metrics.totalLatency.Inc(duration.Seconds(), "", "property", "query")
		if err != nil {
			ps.metrics.totalErr.Inc(1, "", "property", "query")
		}
		// Log query with timing information at the end
		if ps.queryAccessLog != nil {
			if errAccessLog := ps.queryAccessLog.WriteQuery("property", start, duration, req, err); errAccessLog != nil {
				ps.log.Error().Err(errAccessLog).Msg("query access log error")
			}
		}
	}()
	if len(req.Groups) == 0 {
		return nil, schema.BadRequest("groups", "groups should not be empty")
	}
	if req.Limit == 0 {
		req.Limit = 100
	}

	nodeProperties, groups, trace, err := ps.queryProperties(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(nodeProperties) == 0 {
		return &propertyv1.QueryResponse{Properties: nil, Trace: trace}, nil
	}

	var properties []*propertyWithCount

	// Choose processing path based on whether ordering is requested
	if req.OrderBy != nil && req.OrderBy.TagName != "" {
		// Sorted query: Use unified k-way merge with binary search insertion
		properties = ps.sortedQueryWithDedup(nodeProperties, req)
	} else {
		// No ordering: Simple dedup without sorting
		properties = ps.simpleDedupWithoutSort(nodeProperties)
	}

	result := make([]*propertyv1.Property, 0, len(properties))
	for _, property := range properties {
		if err := ps.repairPropertyIfNeed(property.entity, property, groups); err != nil {
			ps.log.Warn().Msgf("failed to repair properties when query: %v", err)
		}
		// ignore deleted property in the query
		if property.deletedTime > 0 {
			continue
		}
		if len(req.TagProjection) > 0 {
			var tags []*modelv1.Tag
			for _, tag := range property.Tags {
				for _, tp := range req.TagProjection {
					if tp == tag.Key {
						tags = append(tags, tag)
						break
					}
				}
			}
			property.Tags = tags
		}
		result = append(result, property.Property)
	}

	// Apply limit
	if len(result) > int(req.Limit) {
		result = result[:req.Limit]
	}
	return &propertyv1.QueryResponse{Properties: result, Trace: trace}, nil
}

// sortedQueryWithDedup implements unified sorted query handling for both single and multi-node scenarios.
// It uses k-way merge with binary search insertion to maintain sorted order efficiently.
func (ps *propertyServer) sortedQueryWithDedup(
	nodeProperties map[string][]*propertyWithMetadata,
	req *propertyv1.QueryRequest,
) []*propertyWithCount {
	// Initialize k-way merge iterators
	iters := make([]sortpkg.Iterator[*propertyWithMetadata], 0, len(nodeProperties))
	for _, props := range nodeProperties {
		if len(props) > 0 {
			iters = append(iters, newPropertyWithMetadataIterator(props))
		}
	}

	if len(iters) == 0 {
		return nil
	}

	// Create heap-based merge iterator
	isDesc := req.OrderBy.Sort == modelv1.Sort_SORT_DESC
	mergeIter := sortpkg.NewItemIter(iters, isDesc)
	defer mergeIter.Close()

	// Initialize deduplication state
	// seenIDs tracks entity -> propertyWithCount for deduplication
	seenIDs := make(map[string]*propertyWithCount)
	// resultBuffer maintains sorted list of unique properties
	resultBuffer := make([]*propertyWithCount, 0, req.Limit)

	// Process items from k-way merge
	for mergeIter.Next() {
		p := mergeIter.Val()
		entity := propertypkg.GetEntity(p.Property)

		// Check if we've seen this entity before
		if existingCount, seen := seenIDs[entity]; seen {
			// Same modRevision - accumulate node
			if p.Metadata.ModRevision == existingCount.Metadata.ModRevision {
				existingCount.addExistNode(p.node)
				continue
			}

			// Older modRevision - skip
			if p.Metadata.ModRevision < existingCount.Metadata.ModRevision {
				continue
			}

			// Newer modRevision - replace old entry
			// Find and remove old entry from resultBuffer using binary search
			oldIndex := ps.findPropertyInBuffer(resultBuffer, existingCount, isDesc)
			if oldIndex >= 0 && oldIndex < len(resultBuffer) {
				// Remove old entry
				resultBuffer = append(resultBuffer[:oldIndex], resultBuffer[oldIndex+1:]...)
			}

			// Create new propertyWithCount for the newer revision
			newCount := newPropertyWithCounts(p, entity, p.node)
			seenIDs[entity] = newCount

			// Insert new entry in sorted position using binary search
			insertPos := ps.findInsertPosition(resultBuffer, newCount, isDesc)
			resultBuffer = ps.insertAtPosition(resultBuffer, newCount, insertPos)

			continue
		}

		// New entity - first time seeing this entity
		// Create propertyWithCount and add to seenIDs
		propCount := newPropertyWithCounts(p, entity, p.node)
		seenIDs[entity] = propCount

		// Insert into resultBuffer at correct sorted position
		insertPos := ps.findInsertPosition(resultBuffer, propCount, isDesc)
		resultBuffer = ps.insertAtPosition(resultBuffer, propCount, insertPos)
	}

	return resultBuffer
}

// findInsertPosition finds the correct position to insert a new property
// in the sorted resultBuffer using binary search.
func (ps *propertyServer) findInsertPosition(
	buffer []*propertyWithCount,
	newProp *propertyWithCount,
	isDesc bool,
) int {
	return sort.Search(len(buffer), func(i int) bool {
		cmp := bytes.Compare(buffer[i].sortedValue, newProp.sortedValue)
		if isDesc {
			return cmp <= 0 // For descending: insert before items <= new value
		}
		return cmp >= 0 // For ascending: insert before items >= new value
	})
}

// insertAtPosition inserts a property at the specified position in the buffer.
func (ps *propertyServer) insertAtPosition(
	buffer []*propertyWithCount,
	prop *propertyWithCount,
	pos int,
) []*propertyWithCount {
	// Grow buffer by one
	buffer = append(buffer, nil)
	// Shift elements to the right
	copy(buffer[pos+1:], buffer[pos:])
	// Insert new element
	buffer[pos] = prop
	return buffer
}

// findPropertyInBuffer finds the index of a specific property in the buffer.
// Returns -1 if not found.
func (ps *propertyServer) findPropertyInBuffer(
	buffer []*propertyWithCount,
	target *propertyWithCount,
	isDesc bool,
) int {
	// Use binary search to find approximate position
	searchPos := sort.Search(len(buffer), func(i int) bool {
		cmp := bytes.Compare(buffer[i].sortedValue, target.sortedValue)
		if isDesc {
			return cmp <= 0
		}
		return cmp >= 0
	})

	// Linear search around the found position to handle duplicate sortedValues
	// (multiple properties might have same sortedValue)
	for i := searchPos; i < len(buffer); i++ {
		if bytes.Equal(buffer[i].sortedValue, target.sortedValue) {
			// Check if it's the exact same property by comparing entity
			if propertypkg.GetEntity(buffer[i].Property) == propertypkg.GetEntity(target.Property) {
				return i
			}
		} else {
			// Moved past matching sortedValues
			break
		}
	}

	// Also check backwards from searchPos (in case binary search landed after our target)
	for i := searchPos - 1; i >= 0; i-- {
		if bytes.Equal(buffer[i].sortedValue, target.sortedValue) {
			if propertypkg.GetEntity(buffer[i].Property) == propertypkg.GetEntity(target.Property) {
				return i
			}
		} else {
			break
		}
	}

	return -1 // Not found
}

// simpleDedupWithoutSort handles queries without ordering.
func (ps *propertyServer) simpleDedupWithoutSort(
	nodeProperties map[string][]*propertyWithMetadata,
) []*propertyWithCount {
	// Collect and deduplicate
	seenIDs := make(map[string]*propertyWithCount)

	for n, props := range nodeProperties {
		for _, p := range props {
			entity := propertypkg.GetEntity(p.Property)

			if existing, seen := seenIDs[entity]; seen {
				switch {
				case existing.Metadata.ModRevision < p.Metadata.ModRevision:
					seenIDs[entity] = newPropertyWithCounts(p, entity, n)
				case existing.Metadata.ModRevision == p.Metadata.ModRevision:
					existing.addExistNode(n)
				}
			} else {
				seenIDs[entity] = newPropertyWithCounts(p, entity, n)
			}
		}
	}

	// Process and collect results
	result := make([]*propertyWithCount, 0, len(seenIDs))
	for _, p := range seenIDs {
		result = append(result, p)
	}

	return result
}

func (ps *propertyServer) repairPropertyIfNeed(entity string, p *propertyWithCount, groups map[string]*commonv1.Group) error {
	// make sure have the enough replicas
	group := groups[p.Metadata.Group]
	if group == nil {
		return errors.Errorf("group %s not found", p.Metadata.Group)
	}
	copies, ok := ps.groupRepo.copies(p.Metadata.GetGroup())
	if !ok {
		return errors.New("failed to get group copies")
	}
	if copies == uint32(len(p.existNodes)) {
		return nil
	}
	shardID, err := partition.ShardID(convert.StringToBytes(entity), group.ResourceOpts.ShardNum)
	if err != nil {
		return err
	}
	nodes := make([]string, 0, copies)
	for i := range copies {
		nodeID, err := ps.nodeRegistry.Locate(p.GetMetadata().GetGroup(),
			p.GetMetadata().GetName(), uint32(shardID), i)
		if err != nil {
			return err
		}
		if _, ok := p.existNodes[nodeID]; !ok {
			nodes = append(nodes, nodeID)
		}
	}
	if len(nodes) == 0 {
		return nil
	}
	ps.repairQueue.addProperty(entity, shardID, p, nodes)
	return nil
}

// queryProperties internal properties query, return all properties with related nodes.
func (ps *propertyServer) queryProperties(
	ctx context.Context,
	req *propertyv1.QueryRequest,
) (nodeProperties map[string][]*propertyWithMetadata, groups map[string]*commonv1.Group, trace *commonv1.Trace, err error) {
	start := time.Now()
	if req.Limit == 0 {
		req.Limit = 100
	}
	var span *query.Span
	if req.Trace {
		tracer, _ := query.NewTracer(ctx, start.Format(time.RFC3339Nano))
		span, _ = tracer.StartSpan(ctx, "property-grpc")
		span.Tag("request", convert.BytesToString(logger.Proto(req)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				trace = tracer.ToProto()
			}
			span.Stop()
		}()
	}
	groups = make(map[string]*commonv1.Group, len(req.Groups))
	for _, gn := range req.Groups {
		g, getGroupErr := ps.schemaRegistry.GroupRegistry().GetGroup(ctx, gn)
		if getGroupErr != nil {
			return nil, nil, trace, errors.Errorf("group %s not found", gn)
		} else if g.Catalog != commonv1.Catalog_CATALOG_PROPERTY {
			return nil, nil, trace, errors.Errorf("group %s is not allowed to have properties", gn)
		}
		groups[gn] = g
	}
	ff, err := ps.pipeline.Broadcast(defaultQueryTimeout, data.TopicPropertyQuery, bus.NewMessage(bus.MessageID(start.Unix()), req))
	if err != nil {
		return nil, nil, trace, err
	}
	res := make(map[string][]*propertyWithMetadata, len(ff))
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			nodeWithProperties := make([]*propertyWithMetadata, 0)
			switch v := d.(type) {
			case *propertyv1.InternalQueryResponse:
				for i, s := range v.Sources {
					var p propertyv1.Property
					var deleteTime int64
					unmarshalErr := protojson.Unmarshal(s, &p)
					if unmarshalErr != nil {
						return nil, groups, trace, unmarshalErr
					}
					if i < len(v.Deletes) {
						deleteTime = v.Deletes[i]
					}

					var sortedValue []byte
					if i < len(v.SortedValues) {
						sortedValue = v.SortedValues[i]
					}

					property := &propertyWithMetadata{
						Property:    &p,
						sortedValue: sortedValue,
						deletedTime: deleteTime,
						node:        m.Node(),
					}
					nodeWithProperties = append(nodeWithProperties, property)
				}
				if span != nil {
					span.AddSubTrace(v.Trace)
				}
				res[m.Node()] = nodeWithProperties
			case *common.Error:
				err = multierr.Append(err, errors.New(v.Error()))
			}
		}
	}
	if err != nil {
		return nil, groups, trace, err
	}
	if len(res) == 0 {
		return res, groups, trace, nil
	}
	return res, groups, trace, nil
}

func (ps *propertyServer) remove(ids [][]byte) error {
	ff, err := ps.pipeline.Broadcast(defaultQueryTimeout, data.TopicPropertyDelete, bus.NewMessage(bus.MessageID(time.Now().Unix()), &propertyv1.InternalDeleteRequest{
		Ids: ids,
	}))
	if err != nil {
		return err
	}
	for _, f := range ff {
		if _, err := f.Get(); err != nil {
			return err
		}
	}
	return nil
}

func (ps *propertyServer) startRepairQueue(stop chan struct{}) {
	ps.repairQueue = newRepairQueue(ps, ps.repairQueueCount)
	ps.repairQueue.Start(stop)
}

type propertyWithMetadata struct {
	*propertyv1.Property
	node        string
	sortedValue []byte
	deletedTime int64
}

// SortedField implements sortpkg.Comparable interface for k-way merge sorting.
func (p *propertyWithMetadata) SortedField() []byte {
	return p.sortedValue
}

// propertyWithMetadataIterator wraps a slice to implement sortpkg.Iterator interface.
type propertyWithMetadataIterator struct {
	data  []*propertyWithMetadata
	index int
}

func newPropertyWithMetadataIterator(data []*propertyWithMetadata) *propertyWithMetadataIterator {
	return &propertyWithMetadataIterator{
		data:  data,
		index: -1,
	}
}

func (it *propertyWithMetadataIterator) Next() bool {
	it.index++
	return it.index < len(it.data)
}

func (it *propertyWithMetadataIterator) Val() *propertyWithMetadata {
	if it.index < 0 || it.index >= len(it.data) {
		return nil
	}
	return it.data[it.index]
}

func (it *propertyWithMetadataIterator) Close() error {
	return nil
}

type propertyWithCount struct {
	*propertyWithMetadata
	existNodes map[string]bool
	entity     string
}

func newPropertyWithCounts(p *propertyWithMetadata, entity, existNode string) *propertyWithCount {
	res := &propertyWithCount{
		propertyWithMetadata: p,
		entity:               entity,
	}
	res.addExistNode(existNode)
	return res
}

func (p *propertyWithCount) addExistNode(node string) {
	if p.existNodes == nil {
		p.existNodes = make(map[string]bool)
	}
	p.existNodes[node] = true
}

type repairQueue struct {
	queue         chan *repairTask
	inProcess     map[repairInProcessKey]struct{}
	ps            *propertyServer
	processLocker sync.Mutex
}

func newRepairQueue(ps *propertyServer, size int) *repairQueue {
	return &repairQueue{
		queue:     make(chan *repairTask, size),
		inProcess: make(map[repairInProcessKey]struct{}, size),
		ps:        ps,
	}
}

func (rq *repairQueue) addProperty(entity string, shardID uint, p *propertyWithCount, nodes []string) bool {
	rq.processLocker.Lock()
	defer rq.processLocker.Unlock()
	key := repairInProcessKey{
		entity:  entity,
		modTime: p.Metadata.ModRevision,
	}
	if _, ok := rq.inProcess[key]; ok {
		return false
	}
	task := &repairTask{
		shardID:  shardID,
		entity:   entity,
		property: p,
		nodes:    nodes,
		key:      key,
	}
	// adding the task to the queue, if the queue is full, then ignore it
	select {
	case rq.queue <- task:
		rq.inProcess[key] = struct{}{}
	default:
		return false
	}
	return true
}

func (rq *repairQueue) Start(stop chan struct{}) {
	go func() {
		for {
			select {
			case <-stop:
				return
			case task := <-rq.queue:
				if err := rq.processTask(context.Background(), task); err != nil {
					rq.ps.log.Warn().Msgf("failed to repair property %s: %v", task.property.Metadata.Name, err)
				}
			}
		}
	}()
}

func (rq *repairQueue) processTask(ctx context.Context, t *repairTask) (err error) {
	rq.ps.metrics.totalStarted.Inc(1, t.property.Metadata.Group, "property", "repair")
	start := time.Now()
	defer func() {
		rq.ps.metrics.totalFinished.Inc(1, t.property.Metadata.Group, "property", "repair")
		rq.ps.metrics.totalLatency.Inc(time.Since(start).Seconds(), t.property.Metadata.Group, "property", "repair")
		if err != nil {
			rq.ps.metrics.totalErr.Inc(1, t.property.Metadata.Group, "property", "repair")
		}
		rq.processLocker.Lock()
		defer rq.processLocker.Unlock()
		delete(rq.inProcess, t.key)
	}()
	futures := make([]bus.Future, 0, len(t.nodes))
	var result error
	// building the repair data
	repairReq := &propertyv1.InternalRepairRequest{
		ShardId:    uint64(t.shardID),
		Id:         propertypkg.GetPropertyID(t.property.Property),
		Property:   t.property.Property,
		DeleteTime: t.property.deletedTime,
	}
	for _, nodeID := range t.nodes {
		f, err := rq.ps.pipeline.Publish(ctx, data.TopicPropertyRepair, bus.NewMessageWithNode(
			bus.MessageID(time.Now().Unix()), nodeID, repairReq))
		if err != nil {
			result = multierr.Append(result, errors.Errorf("failed to repair properties when query: %v", err))
			continue
		}
		futures = append(futures, f)
	}

	// Wait for all futures to complete
	for _, f := range futures {
		if _, err := f.Get(); err != nil {
			result = multierr.Append(result, err)
		}
	}
	return result
}

type repairTask struct {
	property *propertyWithCount
	entity   string
	nodes    []string
	key      repairInProcessKey
	shardID  uint
}

type repairInProcessKey struct {
	entity  string
	modTime int64
}
