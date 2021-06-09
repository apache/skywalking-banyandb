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
	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/logical"
	flatbuffers "github.com/google/flatbuffers/go"
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
		// So far, we only support single-field indices
		indexRules, err := ec.IndexFilter().IndexRules(
			ec,
			createSubject(string(i.params.Metadata().Name()), string(i.params.Metadata().Name())),
			SingleFieldIndex(i.params.KeyName),
		)
		if err != nil {
			return Failure(err)
		}
		indexRuleMetadata := indexRules[0].Metadata(nil)
		cIDs, err := ec.IndexRepo().Search(common.Metadata{
			Spec: *indexRuleMetadata,
		}, sT, eT, i.params.PairQueries)
		if err != nil {
			return Failure(err)
		}
		return Success(NewChunkIDs(cIDs...))
	})
}

func (i *indexScanTransform) AppendParent(f ...Future) {
	i.parents = i.parents.Append(f...)
}

func SingleFieldIndex(fieldName string) series.IndexObjectFilter {
	return func(idxObj apiv1.IndexObject) bool {
		if idxObj.FieldsLength() == 1 && string(idxObj.Fields(0)) == fieldName {
			return true
		}
		return false
	}
}

func createSubject(name, group string) apiv1.Series {
	b := flatbuffers.NewBuilder(0)
	namePos := b.CreateString(name)
	groupPos := b.CreateString(group)
	apiv1.MetadataStart(b)
	apiv1.MetadataAddName(b, namePos)
	apiv1.MetadataAddGroup(b, groupPos)
	s := apiv1.MetadataEnd(b)
	apiv1.IndexRuleStart(b)
	apiv1.SeriesAddCatalog(b, apiv1.CatalogTrace)
	apiv1.SeriesAddSeries(b, s)
	b.Finish(apiv1.IndexRuleEnd(b))
	return *apiv1.GetRootAsSeries(b.FinishedBytes(), 0)
}
