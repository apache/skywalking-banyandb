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
)

// These expected values were cross-checked against the previous release's
// compatibility-oracle analyzer (pkg/index/analyzer.Analyzers, now retired
// along with the third-party library it wrapped) for the same inputs before
// being pinned here as literals; every one matched exactly, each input cloned
// per call so the comparison could not observe one analyzer's input-mutating
// side effect bleeding into another's (the retired oracle's "standard"
// analyzer lowercased its input in place; comparing the two analyzers against
// a shared, unmutated input per call -- not across every analyzer name -- is
// what the "url" analyzer's case-preserving entries below depend on). There
// is no generator test: regenerating this comparison would require the
// retired library, so the literal expectations themselves are the provenance.
func TestOracleParity(t *testing.T) {
	inputs := [][]byte{[]byte("Mixed 123 café 中文 can't"), []byte("the THE repeated repeated"), []byte(""), {0xff, 'A', '-', '2'}}
	want := map[string][]map[string]uint64{
		"keyword": {
			{"Mixed 123 café 中文 can't": 1},
			{"the THE repeated repeated": 1},
			{"": 1},
			{"\xffA-2": 1},
		},
		"simple": {
			{"mixed": 1, "café": 1, "中文": 1, "can": 1, "t": 1},
			{"the": 2, "repeated": 2},
			{},
			{},
		},
		"standard": {
			{"mixed": 1, "123": 1, "café": 1, "中": 1, "文": 1, "can't": 1},
			{"the": 2, "repeated": 2},
			{},
			{"a": 1, "2": 1},
		},
		"url": {
			{"Mixed": 1, "123": 1, "café": 1, "中文": 1, "can": 1, "t": 1},
			{"the": 1, "THE": 1, "repeated": 2},
			{},
			{},
		},
	}
	for _, name := range []string{"keyword", "simple", "standard", "url"} {
		for inputIndex, input := range inputs {
			got, err := Analyze(name, input)
			require.NoError(t, err)
			actual := map[string]uint64{}
			for _, token := range got {
				actual[string(token.Value)] = token.Frequency
			}
			require.Equal(t, want[name][inputIndex], actual, "analyzer=%s input=%q", name, input)
		}
	}
}

// See TestOracleParity's doc comment for how these literals were produced.
func TestOracleTokenSequenceParity(t *testing.T) {
	inputs := [][]byte{
		[]byte("Mixed 123 café 中文 can't"), []byte("the THE repeated repeated"), []byte(""),
		{0xff, 'A', '-', '2'},
		[]byte("http://example.com/a/b?c=d&e=f#g"), []byte("b a b a"),
	}
	want := map[string][][]string{
		"keyword": {
			{"Mixed 123 café 中文 can't"},
			{"the THE repeated repeated"},
			{""},
			{"\xffA-2"},
			{"http://example.com/a/b?c=d&e=f#g"},
			{"b a b a"},
		},
		"simple": {
			{"mixed", "café", "中文", "can", "t"},
			{"the", "the", "repeated", "repeated"},
			{},
			{},
			{"http", "example", "com", "a", "b", "c", "d", "e", "f", "g"},
			{"b", "a", "b", "a"},
		},
		"standard": {
			{"mixed", "123", "café", "中", "文", "can't"},
			{"the", "the", "repeated", "repeated"},
			{},
			{"a", "2"},
			{"http", "example.com", "a", "b", "c", "d", "e", "f", "g"},
			{"b", "a", "b", "a"},
		},
		"url": {
			{"Mixed", "123", "café", "中文", "can", "t"},
			{"the", "THE", "repeated", "repeated"},
			{},
			{},
			{"http", "example", "com", "a", "b", "c", "d", "e", "f", "g"},
			{"b", "a", "b", "a"},
		},
	}
	for _, name := range []string{"keyword", "simple", "standard", "url"} {
		for inputIndex, input := range inputs {
			got, err := Tokens(name, input)
			require.NoError(t, err)
			wantSeq := want[name][inputIndex]
			if len(wantSeq) == 0 {
				require.Empty(t, got, "analyzer=%s input=%q", name, input)
				continue
			}
			require.Equal(t, wantSeq, got, "analyzer=%s input=%q", name, input)
		}
	}
}
