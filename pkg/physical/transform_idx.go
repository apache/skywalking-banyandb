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

package physical

import (
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

var _ Transform = (*indexScanTransform)(nil)

type indexScanTransform struct {
	params  *logical.IndexScan
	parents Futures
}

func NewIndexScanTransform(params *logical.IndexScan) Transform {
	return &indexScanTransform{
		params: params,
	}
}

func (i *indexScanTransform) Run(ec ExecutionContext) Future {
	return NewFuture(func() Result {
		sT, eT := i.params.TimeRange().Begin(), i.params.TimeRange().End()
		cIDs, err := ec.IndexRepo().Search(*i.params.IndexRuleMeta, i.params.FieldNames, sT, eT, i.params.PairQueries)
		if err != nil {
			return Failure(err)
		}
		return Success(NewChunkIDs(cIDs...))
	})
}

func (i *indexScanTransform) AppendParent(f ...Future) {
	i.parents = i.parents.Append(f...)
}
