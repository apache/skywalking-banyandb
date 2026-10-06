// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
package nativeanalysis

import (
	"testing"

	"github.com/stretchr/testify/require"

	legacy "github.com/apache/skywalking-banyandb/pkg/index/analyzer"
)

func TestOracleParity(t *testing.T) {
	inputs := [][]byte{[]byte("Mixed 123 café 中文 can't"), []byte("the THE repeated repeated"), []byte(""), {0xff, 'A', '-', '2'}}
	for _, name := range []string{"keyword", "simple", "standard", "url"} {
		for _, input := range inputs {
			got, err := Analyze(name, input)
			require.NoError(t, err)
			stream := legacy.Analyzers[name].Analyze(input)
			want := map[string]uint64{}
			for _, token := range stream {
				want[string(token.Term)]++
			}
			actual := map[string]uint64{}
			for _, token := range got {
				actual[string(token.Value)] = token.Frequency
			}
			require.Equal(t, want, actual, "analyzer=%s input=%q", name, input)
		}
	}
}

func TestOracleTokenSequenceParity(t *testing.T) {
	inputs := [][]byte{
		[]byte("Mixed 123 café 中文 can't"), []byte("the THE repeated repeated"), []byte(""),
		{0xff, 'A', '-', '2'},
		[]byte("http://example.com/a/b?c=d&e=f#g"), []byte("b a b a"),
	}
	for _, name := range []string{"keyword", "simple", "standard", "url"} {
		for _, input := range inputs {
			got, err := Tokens(name, input)
			require.NoError(t, err)
			stream := legacy.Analyzers[name].Analyze(input)
			want := make([]string, 0, len(stream))
			for _, token := range stream {
				want = append(want, string(token.Term))
			}
			if len(want) == 0 {
				require.Empty(t, got, "analyzer=%s input=%q", name, input)
				continue
			}
			require.Equal(t, want, got, "analyzer=%s input=%q", name, input)
		}
	}
}
