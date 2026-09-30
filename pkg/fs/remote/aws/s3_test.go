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
			name:         "standard URI with prefix (s3://my-bucket/backup-prefix)",
			path:         "my-bucket/backup-prefix",
			wantBucket:   "my-bucket",
			wantBasePath: "backup-prefix",
		},
		{
			name:         "standard URI bucket only (s3://my-bucket)",
			path:         "my-bucket",
			wantBucket:   "my-bucket",
			wantBasePath: "",
		},
		{
			name:         "legacy triple slash URI with prefix (s3:///my-bucket/backup-prefix)",
			path:         "/my-bucket/backup-prefix",
			wantBucket:   "my-bucket",
			wantBasePath: "backup-prefix",
		},
		{
			name:         "legacy triple slash URI bucket only (s3:///my-bucket)",
			path:         "/my-bucket",
			wantBucket:   "my-bucket",
			wantBasePath: "",
		},
		{
			name:         "nested path prefix (s3://my-bucket/dir1/dir2)",
			path:         "my-bucket/dir1/dir2",
			wantBucket:   "my-bucket",
			wantBasePath: "dir1/dir2",
		},
		{
			name:         "empty path",
			path:         "",
			wantBucket:   "",
			wantBasePath: "",
		},
		{
			name:         "slashes only",
			path:         "///",
			wantBucket:   "",
			wantBasePath: "",
		},
		{
			name:         "trailing slash",
			path:         "my-bucket/backup-prefix/",
			wantBucket:   "my-bucket",
			wantBasePath: "backup-prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBucket, gotBasePath := extractBucketAndBase(tt.path)
			if gotBucket != tt.wantBucket {
				t.Errorf("extractBucketAndBase(%q) gotBucket = %q, want %q", tt.path, gotBucket, tt.wantBucket)
			}
			if gotBasePath != tt.wantBasePath {
				t.Errorf("extractBucketAndBase(%q) gotBasePath = %q, want %q", tt.path, gotBasePath, tt.wantBasePath)
			}
		})
	}
}
