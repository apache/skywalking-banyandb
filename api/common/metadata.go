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
	"fmt"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ fmt.Stringer = (*Metadata)(nil)

var MetadataKindVersion = KindVersion{Version: "v1", Kind: "metadata"}

type Metadata struct {
	KindVersion
	Spec v1.Metadata
}

func (metadata *Metadata) String() string {
	return fmt.Sprintf("Metadata{KindVersion=%s/%s,Spec={group=%s,name=%s}}",
		metadata.Kind,
		metadata.Version,
		metadata.Spec.Group(),
		metadata.Spec.Name(),
	)
}
