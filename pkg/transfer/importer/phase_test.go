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

package importer

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhaseWaiterShowsAndErasesTheLiveLine(t *testing.T) {
	var buf bytes.Buffer
	w := phaseWaiter{log: &buf, terminal: func() (int, int) { return 80, 24 }}
	ran := false
	require.NoError(t, w.run(context.Background(), "comparing the schema", func() error {
		ran = true
		return nil
	}))
	assert.True(t, ran)
	out := buf.String()
	assert.Contains(t, out, "comparing the schema (")
	assert.True(t, strings.HasSuffix(out, "\x1b[1A\x1b[2K"), "the live line is erased at the end: %q", out)
}

func TestPhaseWaiterWithoutTerminalPrintsNothing(t *testing.T) {
	var buf bytes.Buffer
	w := phaseWaiter{log: &buf}
	want := errors.New("boom")
	assert.ErrorIs(t, w.run(context.Background(), "comparing the schema", func() error { return want }), want)
	assert.Empty(t, buf.String())
}
