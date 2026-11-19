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

package common

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingCounter map[string]float64

func (c countingCounter) Inc(delta float64, labelValues ...string) {
	c[fmt.Sprint(labelValues)] += delta
}

func (c countingCounter) Delete(_ ...string) bool { return true }

func TestDropReasonFindsTheTagAnywhereInTheChain(t *testing.T) {
	cause := errors.New("group g not found")
	tagged := NewRowDropError(DropReasonGroupNotFound, fmt.Errorf("cannot load tsdb for group g: %w", cause))
	wrapped := fmt.Errorf("stream g/sw: %w", tagged)

	require.Equal(t, DropReasonGroupNotFound, DropReason(tagged))
	require.Equal(t, DropReasonGroupNotFound, DropReason(wrapped), "a caller may wrap the tagged error")
	require.Equal(t, "cannot load tsdb for group g: group g not found", tagged.Error(), "the tag leaves the message as it was")
	require.ErrorIs(t, wrapped, cause, "the tag keeps the cause reachable")
	require.Equal(t, DropReasonInvalidElement, DropReason(errors.New("has no tag family")), "an untagged error is an invalid element")
}

func TestCountRowDrop(t *testing.T) {
	c := countingCounter{}
	CountRowDrop(context.Background(), c, "g", DropReasonDecode)
	CountRowDrop(context.Background(), c, "g", DropReasonDecode)
	require.Equal(t, countingCounter{"[g decode]": 2}, c)

	recorded := WithWriteRecorder(context.Background(), NewWriteRecorder(time.Time{}))
	CountRowDrop(recorded, c, "g", DropReasonDecode)
	require.Equal(t, countingCounter{"[g decode]": 2}, c, "a write with a recorder tells its caller, which may write the row again")

	require.NotPanics(t, func() { CountRowDrop(context.Background(), nil, "g", DropReasonDecode) }, "a listener built without metrics counts nothing")
}
