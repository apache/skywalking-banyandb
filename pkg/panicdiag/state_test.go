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

package panicdiag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBoundedStateWriterWriteJSON(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), deepDumpFileName)
	truncated, err := NewBoundedStateWriter().WriteJSON(path, map[string]string{"pod": "banyand-0"}, 1024)
	if err != nil {
		t.Fatalf("write json state dump: %v", err)
	}
	if truncated {
		t.Fatal("state dump should not be truncated")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state dump: %v", err)
	}
	if !strings.Contains(string(data), `"pod": "banyand-0"`) {
		t.Fatalf("unexpected state dump content: %s", string(data))
	}
}

func TestBoundedStateWriterWriteJSONLimitExceeded(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), deepDumpFileName)
	truncated, err := NewBoundedStateWriter().WriteJSON(path, map[string]string{
		"payload": strings.Repeat("x", 256),
	}, 32)
	if err == nil {
		t.Fatal("expected state dump to exceed limit")
	}
	if !truncated {
		t.Fatal("expected truncated flag when limit is exceeded")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("state dump file should not exist when limit exceeded, stat err: %v", statErr)
	}
}
