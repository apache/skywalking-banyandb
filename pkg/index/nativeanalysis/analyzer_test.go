// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at
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

func TestAnalyzeSemantics(t *testing.T) {
	terms, err := Analyze("keyword", []byte("MiXeD URL"))
	require.NoError(t, err)
	require.Equal(t, []byte("MiXeD URL"), terms[0].Value)
	terms, err = Analyze("simple", []byte("MiXeD CJK 中文 mixed"))
	require.NoError(t, err)
	require.Equal(t, []string{"mixed", "cjk", "中文"}, values(terms))
	terms, err = Analyze("url", []byte("HTTP://A-1/x"))
	require.NoError(t, err)
	require.Equal(t, []string{"HTTP", "A", "1", "x"}, values(terms))
	terms, err = Analyze("simple", []byte("A a a"))
	require.NoError(t, err)
	require.Equal(t, uint64(3), terms[0].Frequency)
}

func values(ts []Term) []string {
	r := make([]string, len(ts))
	for i := range ts {
		r[i] = string(ts[i].Value)
	}
	return r
}
