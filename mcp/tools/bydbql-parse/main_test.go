// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateContract(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		wantValid     bool
		wantQueryType string
	}{
		{name: "select stream", query: "SELECT * FROM STREAM sw IN default TIME > '-30m'", wantValid: true, wantQueryType: "STREAM"},
		{name: "select measure", query: "SELECT service, MEAN(latency) FROM MEASURE m IN g TIME > '-30m' GROUP BY service", wantValid: true, wantQueryType: "MEASURE"},
		{name: "select trace", query: "SELECT () FROM TRACE sw_trace IN default TIME > '-30m'", wantValid: true, wantQueryType: "TRACE"},
		{name: "select property", query: "SELECT * FROM PROPERTY server_metadata IN datacenter-1 WHERE ID = 'server-1'", wantValid: true, wantQueryType: "PROPERTY"},
		{name: "show top", query: "SHOW TOP 10 FROM MEASURE m IN g TIME > '-30m' AGGREGATE BY MEAN ORDER BY DESC", wantValid: true, wantQueryType: "TOPN"},
		{name: "reject delete", query: "DELETE FROM STREAM sw IN default", wantValid: false},
		{name: "reject trailing semicolon", query: "SELECT * FROM STREAM sw IN default;", wantValid: false},
		{name: "reject line comment", query: "SELECT * FROM STREAM sw IN default -- comment", wantValid: false},
		{name: "reject two statements", query: "SELECT * FROM STREAM sw IN default SELECT * FROM STREAM sw IN default", wantValid: false},
		{name: "reject empty input", query: "", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := validate(tt.query)
			if resp.Valid != tt.wantValid {
				t.Fatalf("validate(%q).Valid = %t, want %t (message: %s)", tt.query, resp.Valid, tt.wantValid, resp.Message)
			}
			if !resp.SyntaxOnly {
				t.Errorf("validate(%q).SyntaxOnly = false, want true", tt.query)
			}
			if !tt.wantValid {
				return
			}
			if !strings.EqualFold(resp.QueryType, tt.wantQueryType) {
				t.Errorf("validate(%q).QueryType = %q, want %q (case-insensitive)", tt.query, resp.QueryType, tt.wantQueryType)
			}
			if len(resp.Warnings) == 0 {
				t.Errorf("validate(%q) returned no parse-only warning", tt.query)
			}
		})
	}
}

func TestValidateGoldenCorpus(t *testing.T) {
	for _, corpus := range []struct {
		file      string
		wantValid bool
	}{
		{file: "valid.txt", wantValid: true},
		{file: "invalid.txt", wantValid: false},
	} {
		queries := readCorpus(t, filepath.Join("testdata", corpus.file))
		if len(queries) == 0 {
			t.Fatalf("corpus %s is empty", corpus.file)
		}
		for _, query := range queries {
			resp := validate(query)
			if resp.Valid != corpus.wantValid {
				t.Errorf("[%s] validate(%q).Valid = %t, want %t (message: %s)", corpus.file, query, resp.Valid, corpus.wantValid, resp.Message)
			}
		}
	}
}

func readCorpus(t *testing.T, path string) []string {
	t.Helper()
	file, openErr := os.Open(path)
	if openErr != nil {
		t.Fatalf("failed to open corpus %s: %v", path, openErr)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Errorf("failed to close corpus %s: %v", path, closeErr)
		}
	}()

	var queries []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		queries = append(queries, line)
	}
	if scanErr := scanner.Err(); scanErr != nil {
		t.Fatalf("failed to read corpus %s: %v", path, scanErr)
	}

	return queries
}
