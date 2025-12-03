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

package logical

import modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"

// CollectCriteriaTagNames walks through the provided criteria expression and
// records every referenced tag name into the supplied map. The map acts as a set
// that deduplicates tag names for callers that need to know all tags used in a
// criteria tree.
func CollectCriteriaTagNames(criteria *modelv1.Criteria, tags map[string]struct{}) {
	if criteria == nil || tags == nil {
		return
	}
	switch exp := criteria.GetExp().(type) {
	case *modelv1.Criteria_Condition:
		if cond := criteria.GetCondition(); cond != nil {
			tags[cond.GetName()] = struct{}{}
		}
	case *modelv1.Criteria_Le:
		if exp.Le == nil {
			return
		}
		CollectCriteriaTagNames(exp.Le.Left, tags)
		CollectCriteriaTagNames(exp.Le.Right, tags)
	}
}
