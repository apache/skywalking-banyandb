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

// Package native provides a simple meter system for metrics. The metrics are aggregated by the meter provider.
package native

import (
	"context"
	"errors"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

const (
	// ObservabilityGroupName is the native observability group name.
	ObservabilityGroupName = "_monitoring"
	defaultTagFamily       = "default"
	defaultFieldName       = "value"
	tagNodeType            = "node_type"
	tagNodeID              = "node_id"
	tagGRPCAddress         = "grpc_address"
	tagHTTPAddress         = "http_address"
)

var log = logger.GetLogger("observability", "metrics", "system")

// NodeInfo is the struct that contains information used in native observability mode.
type NodeInfo struct {
	Type        string
	NodeID      string
	GrpcAddress string
	HTTPAddress string
}

type provider struct {
	metadata metadata.Repo
	scope    meter.Scope
	nodeInfo NodeInfo
}

// NewProvider returns a native metrics Provider.
func NewProvider(ctx context.Context, scope meter.Scope, metadata metadata.Repo, nodeInfo NodeInfo) meter.Provider {
	p := &provider{
		scope:    scope,
		metadata: metadata,
		nodeInfo: nodeInfo,
	}
	err := p.createNativeObservabilityGroup(ctx)
	if err != nil && !errors.Is(err, schema.ErrGRPCAlreadyExists) {
		log.Error().Err(err).Msg("Failed to create native observability group")
	}
	return p
}

// Counter returns a native implementation of the Counter interface.
func (p *provider) Counter(name string, labelNames ...string) meter.Counter {
	name, err := p.createMeasure(name, labelNames...)
	if err != nil && !errors.Is(err, schema.ErrGRPCAlreadyExists) {
		log.Error().Err(err).Msgf("Failure to createMeasure for Counter %s, labels: %v", name, labelNames)
	}
	return &Counter{
		newMetricVec(name, p.scope, p.nodeInfo),
	}
}

// Gauge returns a nativeimplementation of the Gauge interface.
func (p *provider) Gauge(name string, labelNames ...string) meter.Gauge {
	name, err := p.createMeasure(name, labelNames...)
	if err != nil && !errors.Is(err, schema.ErrGRPCAlreadyExists) {
		log.Error().Err(err).Msgf("Failure to createMeasure for Gauge %s, labels: %v", name, labelNames)
	}
	return &Gauge{
		newMetricVec(name, p.scope, p.nodeInfo),
	}
}

// Histogram returns a native implementation of the Histogram interface.
func (p *provider) Histogram(name string, _ meter.Buckets, _ ...string) meter.Histogram {
	return &Histogram{
		newMetricVec(name, p.scope, p.nodeInfo),
	}
}

func (p *provider) createNativeObservabilityGroup(ctx context.Context) error {
	g := &commonv1.Group{
		Metadata: &commonv1.Metadata{
			Name: ObservabilityGroupName,
		},
		Catalog: commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum: 1,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  1,
			},
		},
	}
	return p.metadata.GroupRegistry().CreateGroup(ctx, g)
}

func (p *provider) createMeasure(metric string, labels ...string) (string, error) {
	tags, entityTags := buildTags(p.scope, labels)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.metadata.MeasureRegistry().CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  metric,
			Group: ObservabilityGroupName,
		},
		Entity: &databasev1.Entity{
			TagNames: entityTags,
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: defaultTagFamily,
				Tags: tags,
			},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              defaultFieldName,
				FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
	})
	return metric, err
}

func buildTags(scope meter.Scope, labels []string) ([]*databasev1.TagSpec, []string) {
	var tags []*databasev1.TagSpec
	var entityTags []string
	addTags := func(labels ...string) {
		for _, label := range labels {
			tag := &databasev1.TagSpec{
				Name: label,
				Type: databasev1.TagType_TAG_TYPE_STRING,
			}
			tags = append(tags, tag)
			entityTags = append(entityTags, label)
		}
	}
	addTags(tagNodeType)
	addTags(tagNodeID)
	addTags(tagGRPCAddress)
	addTags(tagHTTPAddress)
	for label := range scope.GetLabels() {
		addTags(label)
	}
	addTags(labels...)
	return tags, entityTags
}

func buildTagValues(nodeInfo NodeInfo, scope meter.Scope, labelValues ...string) []*modelv1.TagValue {
	var tagValues []*modelv1.TagValue
	addTagValues := func(labelValues ...string) {
		for _, value := range labelValues {
			tagValue := &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: value,
					},
				},
			}
			tagValues = append(tagValues, tagValue)
		}
	}
	addTagValues(nodeInfo.Type)
	addTagValues(nodeInfo.NodeID)
	addTagValues(nodeInfo.GrpcAddress)
	addTagValues(nodeInfo.HTTPAddress)
	for _, labelValue := range scope.GetLabels() {
		addTagValues(labelValue)
	}
	addTagValues(labelValues...)
	return tagValues
}
