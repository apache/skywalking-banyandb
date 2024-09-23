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

package inverted

import (
	"testing"

	"github.com/blugelabs/bluge/analysis"
	"github.com/stretchr/testify/assert"
)

func TestAlphanumericFilter(t *testing.T) {
	filter := newAlphanumericFilter()

	tests := []struct {
		input    analysis.TokenStream
		expected analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("hello123"),
				},
			},
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("hello123"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("hello!@#"),
				},
			},
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("hello"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("123!@#"),
				},
			},
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("123"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("!@#"),
				},
			},
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte(""),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.input[0].Term), func(t *testing.T) {
			output := filter.Filter(tt.input)
			assert.Equal(t, tt.expected, output)
		})
	}
}

func TestNewURLAnalyzer(t *testing.T) {
	analyzer := newURLAnalyzer()

	tests := []struct {
		input    string
		expected analysis.TokenStream
	}{
		{
			input: "http://example.com",
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("http"),
				},
				&analysis.Token{
					Term: []byte("example"),
				},
				&analysis.Token{
					Term: []byte("com"),
				},
			},
		},
		{
			input: "https://www.example.com/path?query=123",
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("https"),
				},
				&analysis.Token{
					Term: []byte("www"),
				},
				&analysis.Token{
					Term: []byte("example"),
				},
				&analysis.Token{
					Term: []byte("com"),
				},
				&analysis.Token{
					Term: []byte("path"),
				},
				&analysis.Token{
					Term: []byte("query"),
				},
				&analysis.Token{
					Term: []byte("123"),
				},
			},
		},
		{
			input: "ftp://user:pass@ftp.example.com:21/path",
			expected: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("ftp"),
				},
				&analysis.Token{
					Term: []byte("user"),
				},
				&analysis.Token{
					Term: []byte("pass"),
				},
				&analysis.Token{
					Term: []byte("ftp"),
				},
				&analysis.Token{
					Term: []byte("example"),
				},
				&analysis.Token{
					Term: []byte("com"),
				},
				&analysis.Token{
					Term: []byte("21"),
				},
				&analysis.Token{
					Term: []byte("path"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokenStream := analyzer.Analyze([]byte(tt.input))
			assert.Equal(t, extractTerms(tt.expected), extractTerms(tokenStream))
		})
	}
}

func extractTerms(tokenStream analysis.TokenStream) [][]byte {
	terms := make([][]byte, len(tokenStream))
	for i, token := range tokenStream {
		terms[i] = token.Term
	}
	return terms
}
