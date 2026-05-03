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

package measure

import "testing"

func TestVectorizedConfig_Default_Disabled_BatchSize1024_Memory256(t *testing.T) {
	c := DefaultConfig()
	if c.Enabled {
		t.Fatal("default Enabled must be false (v1 ships disabled)")
	}
	if c.BatchSize != 1024 {
		t.Fatalf("default BatchSize: want 1024, got %d", c.BatchSize)
	}
	if c.QueryMemoryMiB != 256 {
		t.Fatalf("default QueryMemoryMiB: want 256, got %d", c.QueryMemoryMiB)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("default config must Validate cleanly, got %v", err)
	}
}

func TestVectorizedConfig_Validate_ZeroBatchSize_ReturnsError(t *testing.T) {
	c := DefaultConfig()
	c.BatchSize = 0
	if err := c.Validate(); err == nil {
		t.Fatal("BatchSize=0 must fail Validate")
	}
}

func TestVectorizedConfig_Validate_NegativeMemoryMiB_ReturnsError(t *testing.T) {
	c := DefaultConfig()
	c.QueryMemoryMiB = -1
	if err := c.Validate(); err == nil {
		t.Fatal("negative QueryMemoryMiB must fail Validate")
	}
}
