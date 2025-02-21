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

package data

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// PropertyUpdateKindVersion is the version tag of property update kind.
var PropertyUpdateKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "property-update",
}

// TopicPropertyUpdate is the property update topic.
var TopicPropertyUpdate = bus.BiTopic(PropertyUpdateKindVersion.String())

// PropertyDeleteKindVersion is the version tag of property delete kind.
var PropertyDeleteKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "property-delete",
}

// TopicPropertyDelete is the property update topic.
var TopicPropertyDelete = bus.BiTopic(PropertyDeleteKindVersion.String())

// PropertyQueryKindVersion is the version tag of property query kind.
var PropertyQueryKindVersion = common.KindVersion{
	Version: "v1",
	Kind:    "property-query",
}

// TopicPropertyQuery is the property query topic.
var TopicPropertyQuery = bus.BiTopic(PropertyQueryKindVersion.String())
