// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNativeExactTermsCancelsBetweenDocuments proves that cancellation after a
// successful visit prevents the next selected document from being decoded or
// delivered.
func TestNativeExactTermsCancelsBetweenDocuments(t *testing.T) {
	tester := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	visited := 0
	err := ReadOnlySelectDocuments(ctx, nidx01cSourceADir,
		identitySelection(nidx01dIdentity101, nidx01dIdentity202), func(_ StoredDocument) error {
			visited++
			if visited == 1 {
				cancel()
			}
			return nil
		})

	tester.ErrorIs(err, context.Canceled)
	tester.Equal(1, visited, "cancellation after the first visit must prevent the second selected document")
}
