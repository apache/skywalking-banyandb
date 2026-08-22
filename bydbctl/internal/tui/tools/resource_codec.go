// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
// ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

package tools

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

type resourceCodec struct {
	summarize  func(SchemaRequest, []byte, time.Time) (session.SchemaSnapshot, error)
	decodeList func([]byte) ([]string, error)
	schemaPath string
	listPath   string
	catalog    commonv1.Catalog
}

var resourceTypeOrder = []session.ResourceType{
	session.ResourceTypeMeasure,
	session.ResourceTypeStream,
	session.ResourceTypeTrace,
	session.ResourceTypeProperty,
	session.ResourceTypeTopN,
}

var resourceCodecs = map[session.ResourceType]resourceCodec{
	session.ResourceTypeMeasure: {
		summarize: summarizeMeasureSchema,
		decodeList: decodeNames(
			func() *databasev1.MeasureRegistryServiceListResponse {
				return new(databasev1.MeasureRegistryServiceListResponse)
			},
			(*databasev1.MeasureRegistryServiceListResponse).GetMeasure,
			(*databasev1.Measure).GetMetadata,
		),
		schemaPath: measureSchemaPath, listPath: measureListPath, catalog: commonv1.Catalog_CATALOG_MEASURE,
	},
	session.ResourceTypeStream: {
		summarize: summarizeStreamSchema,
		decodeList: decodeNames(
			func() *databasev1.StreamRegistryServiceListResponse {
				return new(databasev1.StreamRegistryServiceListResponse)
			},
			(*databasev1.StreamRegistryServiceListResponse).GetStream,
			(*databasev1.Stream).GetMetadata,
		),
		schemaPath: streamSchemaPath, listPath: streamListPath, catalog: commonv1.Catalog_CATALOG_STREAM,
	},
	session.ResourceTypeTrace: {
		summarize: summarizeTraceSchema,
		decodeList: decodeNames(
			func() *databasev1.TraceRegistryServiceListResponse {
				return new(databasev1.TraceRegistryServiceListResponse)
			},
			(*databasev1.TraceRegistryServiceListResponse).GetTrace,
			(*databasev1.Trace).GetMetadata,
		),
		schemaPath: traceSchemaPath, listPath: traceListPath, catalog: commonv1.Catalog_CATALOG_TRACE,
	},
	session.ResourceTypeProperty: {
		summarize: summarizePropertySchema,
		decodeList: decodeNames(
			func() *databasev1.PropertyRegistryServiceListResponse {
				return new(databasev1.PropertyRegistryServiceListResponse)
			},
			(*databasev1.PropertyRegistryServiceListResponse).GetProperties,
			(*databasev1.Property).GetMetadata,
		),
		schemaPath: propertySchemaPath, listPath: propertyListPath, catalog: commonv1.Catalog_CATALOG_PROPERTY,
	},
	session.ResourceTypeTopN: {
		summarize: summarizeTopNSchema,
		decodeList: decodeNames(
			func() *databasev1.TopNAggregationRegistryServiceListResponse {
				return new(databasev1.TopNAggregationRegistryServiceListResponse)
			},
			(*databasev1.TopNAggregationRegistryServiceListResponse).GetTopNAggregation,
			(*databasev1.TopNAggregation).GetMetadata,
		),
		schemaPath: topnSchemaPath, listPath: topnListPath, catalog: commonv1.Catalog_CATALOG_UNSPECIFIED,
	},
}

func resourceCodecFor(resourceType session.ResourceType) (resourceCodec, error) {
	codec, found := resourceCodecs[resourceType]
	if !found {
		return resourceCodec{}, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
	return codec, nil
}

func catalogResourceTypes() []session.ResourceType {
	return append([]session.ResourceType(nil), resourceTypeOrder...)
}

func resourceCatalog(resourceType session.ResourceType) commonv1.Catalog {
	codec, found := resourceCodecs[resourceType]
	if !found {
		return commonv1.Catalog_CATALOG_UNSPECIFIED
	}
	return codec.catalog
}

func schemaPath(resourceType session.ResourceType) (string, error) {
	codec, codecErr := resourceCodecFor(resourceType)
	if codecErr != nil {
		return "", codecErr
	}
	return codec.schemaPath, nil
}

func resourceListPath(resourceType session.ResourceType) (string, error) {
	codec, codecErr := resourceCodecFor(resourceType)
	if codecErr != nil {
		return "", codecErr
	}
	return codec.listPath, nil
}

func resourceNamesFromList(resourceType session.ResourceType, body []byte) ([]string, error) {
	codec, codecErr := resourceCodecFor(resourceType)
	if codecErr != nil {
		return nil, codecErr
	}
	return codec.decodeList(body)
}

// decodeNames decodes one registry list response and returns its distinct resource names.
func decodeNames[L proto.Message, R any](newList func() L, resourcesOf func(L) []*R, metadataOf func(*R) *commonv1.Metadata) func([]byte) ([]string, error) {
	return func(body []byte) ([]string, error) {
		listResponse := newList()
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, fmt.Errorf("failed to decode resource registry list: %w", unmarshalErr)
		}
		return metadataNames(collectMetadata(resourcesOf(listResponse), metadataOf)), nil
	}
}
