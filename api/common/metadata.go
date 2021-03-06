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

package common

import (
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

var MetadataKindVersion = KindVersion{Version: "commonv1", Kind: "metadata"}

type Metadata struct {
	KindVersion
	Spec *commonv1.Metadata
}

func (md Metadata) Equal(other Metadata) bool {
	return md.KindVersion.Kind == other.KindVersion.Kind && md.KindVersion.Version == other.KindVersion.Version &&
		md.Spec.Group == other.Spec.Group &&
		md.Spec.Name == other.Spec.Name
}

func NewMetadata(spec *commonv1.Metadata) *Metadata {
	return &Metadata{
		KindVersion: MetadataKindVersion,
		Spec:        spec,
	}
}

func NewMetadataByNameAndGroup(name, group string) *Metadata {
	return &Metadata{
		KindVersion: MetadataKindVersion,
		Spec: &commonv1.Metadata{
			Name:  name,
			Group: group,
		},
	}
}
