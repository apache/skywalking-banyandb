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

// Package valuetype holds the ValueType byte enum used to tag encoded tag and
// field values. It is a dependency-free leaf: it imports nothing, so packages
// that need only the type tag (e.g. the plugin SDK at pkg/pipeline/sdk) can
// reference it without transitively pulling in the heavier pkg/pb/v1 package
// and its logging/encoding dependencies. pkg/pb/v1 re-exports these symbols as
// aliases, so existing pbv1.ValueType / pbv1.ValueType* usages are unaffected.
package valuetype

// ValueType is the type of the tag and field value.
type ValueType byte

// ValueType constants.
const (
	ValueTypeUnknown ValueType = iota
	ValueTypeStr
	ValueTypeInt64
	ValueTypeFloat64
	ValueTypeBinaryData
	ValueTypeStrArr
	ValueTypeInt64Arr
	ValueTypeTimestamp
)
