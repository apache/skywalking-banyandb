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

import (
	"strings"
)

func Format(p Plan) string {
	return formatWithIndent(p, 0)
}

func formatWithIndent(p Plan, indent int) string {
	res := ""
	if indent > 1 {
		res += strings.Repeat(" ", 5*(indent-1))
	}
	if indent > 0 {
		res += "+"
		res += strings.Repeat("-", 4)
	}
	res += p.String() + "\n"
	for _, child := range p.Children() {
		res += formatWithIndent(child, indent+1)
	}
	return res
}

func formatTagRefs(sep string, exprGroup ...[]*TagRef) string {
	var exprsStr []string
	for _, exprs := range exprGroup {
		for i := 0; i < len(exprs); i++ {
			exprsStr = append(exprsStr, exprs[i].String())
		}
	}
	return strings.Join(exprsStr, sep)
}
