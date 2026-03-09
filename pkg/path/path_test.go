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

package path

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGet(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:    "empty string",
			input:   "",
			want:    "",
			wantErr: false,
		},
		{
			name:    "absolute path",
			input:   "/tmp/test",
			want:    filepath.Clean("/tmp/test"),
			wantErr: false,
		},
		{
			name:    "relative path",
			input:   "test/dir",
			want:    filepath.Join(wd, "test/dir"),
			wantErr: false,
		},
		{
			name:    "relative path with dot",
			input:   "./test/dir",
			want:    filepath.Join(wd, "test/dir"),
			wantErr: false,
		},
		{
			name:    "relative path with parent",
			input:   "../test/dir",
			want:    filepath.Join(filepath.Dir(wd), "test/dir"),
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Get(test.input)
			if (err != nil) != test.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if got != test.want {
				t.Errorf("Get() = %v, want %v", got, test.want)
			}
		})
	}
}
