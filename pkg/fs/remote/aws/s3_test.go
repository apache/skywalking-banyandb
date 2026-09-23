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

package aws

import (
	"testing"
)

func TestExtractBucketAndBase(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		wantBucket   string
		wantBasePath string
	}{
		{
			name:         "empty path",
			path:         "",
			wantBucket:   "",
			wantBasePath: "",
		},
		{
			name:         "bucket only",
			path:         "my-bucket",
			wantBucket:   "my-bucket",
			wantBasePath: "",
		},
		{
			name:         "bucket with leading and trailing slashes",
			path:         "/my-bucket/",
			wantBucket:   "my-bucket",
			wantBasePath: "",
		},
		{
			name:         "bucket and prefix",
			path:         "my-bucket/backup/snapshots",
			wantBucket:   "my-bucket",
			wantBasePath: "backup/snapshots",
		},
		{
			name:         "bucket and prefix with slashes",
			path:         "/my-bucket/backup/snapshots/",
			wantBucket:   "my-bucket",
			wantBasePath: "backup/snapshots",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotBasePath := extractBucketAndBase(tt.path)
			if gotBucket != tt.wantBucket {
				t.Errorf("extractBucketAndBase() gotBucket = %q, want %q", gotBucket, tt.wantBucket)
			}
			if gotBasePath != tt.wantBasePath {
				t.Errorf("extractBucketAndBase() gotBasePath = %q, want %q", gotBasePath, tt.wantBasePath)
			}
		})
	}
}

func TestGetFullPath(t *testing.T) {
	tests := []struct {
		name     string
		basePath string
		input    string
		want     string
	}{
		{
			name:     "empty base path with relative input",
			basePath: "",
			input:    "snapshots/file.data",
			want:     "snapshots/file.data",
		},
		{
			name:     "empty base path with leading slash input",
			basePath: "",
			input:    "/snapshots/file.data",
			want:     "snapshots/file.data",
		},
		{
			name:     "configured base path with relative input",
			basePath: "backup/2026",
			input:    "snapshots/file.data",
			want:     "backup/2026/snapshots/file.data",
		},
		{
			name:     "configured base path with leading slash input",
			basePath: "backup/2026",
			input:    "/snapshots/file.data",
			want:     "backup/2026/snapshots/file.data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &s3FS{basePath: tt.basePath}
			got := fs.getFullPath(tt.input)
			if got != tt.want {
				t.Errorf("getFullPath() = %q, want %q", got, tt.want)
			}
		})
	}
}
