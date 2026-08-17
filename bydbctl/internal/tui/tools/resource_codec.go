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
		summarize: summarizeMeasureSchema, decodeList: decodeMeasureNames,
		schemaPath: measureSchemaPath, listPath: measureListPath, catalog: commonv1.Catalog_CATALOG_MEASURE,
	},
	session.ResourceTypeStream: {
		summarize: summarizeStreamSchema, decodeList: decodeStreamNames,
		schemaPath: streamSchemaPath, listPath: streamListPath, catalog: commonv1.Catalog_CATALOG_STREAM,
	},
	session.ResourceTypeTrace: {
		summarize: summarizeTraceSchema, decodeList: decodeTraceNames,
		schemaPath: traceSchemaPath, listPath: traceListPath, catalog: commonv1.Catalog_CATALOG_TRACE,
	},
	session.ResourceTypeProperty: {
		summarize: summarizePropertySchema, decodeList: decodePropertyNames,
		schemaPath: propertySchemaPath, listPath: propertyListPath, catalog: commonv1.Catalog_CATALOG_PROPERTY,
	},
	session.ResourceTypeTopN: {
		summarize: summarizeTopNSchema, decodeList: decodeTopNNames,
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

func decodeMeasureNames(body []byte) ([]string, error) {
	listResponse := new(databasev1.MeasureRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractMeasureMetadata(listResponse.GetMeasure())), nil
}

func decodeStreamNames(body []byte) ([]string, error) {
	listResponse := new(databasev1.StreamRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractStreamMetadata(listResponse.GetStream())), nil
}

func decodeTraceNames(body []byte) ([]string, error) {
	listResponse := new(databasev1.TraceRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractTraceMetadata(listResponse.GetTrace())), nil
}

func decodePropertyNames(body []byte) ([]string, error) {
	listResponse := new(databasev1.PropertyRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractPropertyMetadata(listResponse.GetProperties())), nil
}

func decodeTopNNames(body []byte) ([]string, error) {
	listResponse := new(databasev1.TopNAggregationRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractTopNMetadata(listResponse.GetTopNAggregation())), nil
}
