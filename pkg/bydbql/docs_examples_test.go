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

package bydbql_test

import (
	"fmt"
	"os"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/apache/skywalking-banyandb/pkg/bydbql"
)

const bydbqlDocPath = "../../docs/interacting/bydbql.md"

type docExample struct {
	query string
	line  int
}

// stripSQLComment drops a trailing "--" comment that is not inside a quoted literal.
func stripSQLComment(line string) string {
	var quote byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '-' && i+1 < len(line) && line[i+1] == '-':
			return line[:i]
		}
	}
	return line
}

// docExamples returns the full statements in the sql blocks of the BydbQL doc.
// Blocks that only list clause fragments are skipped as they don't start with SELECT or SHOW.
func docExamples(doc string) []docExample {
	var examples []docExample
	var stmt strings.Builder
	start, inSQL := 0, false
	flush := func() {
		q := strings.TrimSpace(stmt.String())
		stmt.Reset()
		upper := strings.ToUpper(q)
		if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") {
			examples = append(examples, docExample{query: q, line: start})
		}
	}
	for i, raw := range strings.Split(doc, "\n") {
		trimmed := strings.TrimSpace(raw)
		if !inSQL {
			inSQL = trimmed == "```sql"
			continue
		}
		if trimmed == "```" {
			flush()
			inSQL = false
			continue
		}
		line := stripSQLComment(raw)
		for {
			idx := strings.IndexByte(line, ';')
			part := line
			if idx >= 0 {
				part = line[:idx]
			}
			if strings.TrimSpace(part) != "" {
				if strings.TrimSpace(stmt.String()) == "" {
					start = i + 1
				}
				stmt.WriteString(part)
				stmt.WriteString("\n")
			}
			if idx < 0 {
				break
			}
			flush()
			line = line[idx+1:]
		}
	}
	return examples
}

var _ = Describe("BydbQL doc examples", func() {
	doc, readErr := os.ReadFile(bydbqlDocPath)

	It("reads the doc", func() {
		Expect(readErr).NotTo(HaveOccurred())
		Expect(docExamples(string(doc))).NotTo(BeEmpty())
	})

	for _, ex := range docExamples(string(doc)) {
		It(fmt.Sprintf("parses the example at bydbql.md:%d", ex.line), func() {
			_, err := ParseQuery(ex.query)
			Expect(err).NotTo(HaveOccurred(), "query:\n%s", ex.query)
		})
	}
})
