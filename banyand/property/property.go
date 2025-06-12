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

// Package property provides the property service interface.
package property

import (
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/run"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// Service is the interface for property service.
type Service interface {
	run.PreRunner
	run.Config
	run.Service
}

func GetPropertyID(prop *propertyv1.Property) []byte {
	return convert.StringToBytes(GetEntity(prop) + "/" + strconv.FormatInt(prop.Metadata.ModRevision, 10))
}

func GetEntity(prop *propertyv1.Property) string {
	return strings.Join([]string{prop.Metadata.Group, prop.Metadata.Name, prop.Id}, "/")
}
