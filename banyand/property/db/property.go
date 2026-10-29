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

package db

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// GetPropertyID returns the property ID based on the property metadata and revision.
func GetPropertyID(prop *propertyv1.Property) []byte {
	return convert.StringToBytes(GetEntity(prop) + "/" + strconv.FormatInt(prop.Metadata.ModRevision, 10))
}

// GetEntity returns the entity string for the property.
func GetEntity(prop *propertyv1.Property) string {
	return strings.Join([]string{prop.Metadata.Group, prop.Metadata.Name, prop.Id}, "/")
}

// DecodeStoredDocument rebuilds a property and its deletion time from the stored fields of
// a property index document. A live property stores no deletion field, so it decodes to 0.
func DecodeStoredDocument(doc native.StoredDocument) (*propertyv1.Property, int64, error) {
	var (
		property   *propertyv1.Property
		deleteTime int64
		decodeErr  error
	)
	if err := doc.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case sourceField:
			property = &propertyv1.Property{}
			if err := protojson.Unmarshal(value, property); err != nil {
				decodeErr = fmt.Errorf("unmarshal property source: %w: %w", err, native.ErrCorrupt)
				return false
			}
		case deleteField:
			if len(value) != 8 {
				decodeErr = fmt.Errorf("invalid deletion timestamp length %d: %w", len(value), native.ErrCorrupt)
				return false
			}
			deleteTime = convert.BytesToInt64(value)
		}
		return true
	}); err != nil {
		return nil, 0, err
	}
	if decodeErr != nil {
		return nil, 0, decodeErr
	}
	// An empty group, name or id would make Repair match every revision sharing the rest.
	if property.GetMetadata().GetGroup() == "" || property.GetMetadata().GetName() == "" || property.GetId() == "" {
		return nil, 0, fmt.Errorf("property document lacks its group, name or id: %w", native.ErrCorrupt)
	}
	return property, deleteTime, nil
}
