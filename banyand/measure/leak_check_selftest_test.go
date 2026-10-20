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

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingTB captures what verifyNoScanLeaks would report, so the check itself is
// tested: a leak check that never fails would pass every suite.
type recordingTB struct {
	testing.TB
	cleanups []func()
	errs     []string
}

func (r *recordingTB) Helper()                   {}
func (r *recordingTB) Cleanup(f func())          { r.cleanups = append(r.cleanups, f) }
func (r *recordingTB) Errorf(f string, a ...any) { r.errs = append(r.errs, fmt.Sprintf(f, a...)) }

func (r *recordingTB) finish() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

func TestVerifyNoScanLeaksReportsEachKindOfLeak(t *testing.T) {
	prev := scanLeakSettle
	scanLeakSettle = 50 * time.Millisecond
	defer func() { scanLeakSettle = prev }()

	t.Run("clean", func(t *testing.T) {
		r := &recordingTB{TB: t}
		verifyNoScanLeaks(r)
		bc := generateBlockCursor()
		releaseBlockCursor(bc)
		r.finish()
		require.Empty(t, r.errs)
	})
	t.Run("cursor still referenced", func(t *testing.T) {
		r := &recordingTB{TB: t}
		verifyNoScanLeaks(r)
		bc := generateBlockCursor()
		r.finish()
		releaseBlockCursor(bc)
		require.Len(t, r.errs, 1)
	})
	t.Run("reservation still held", func(t *testing.T) {
		r := &recordingTB{TB: t}
		verifyNoScanLeaks(r)
		budget := newScanBudget(-1, nil)
		var res scanReservation
		require.Equal(t, reserveOK, budget.tryReserve(100, false, &res))
		r.finish()
		res.release()
		require.Len(t, r.errs, 1)
	})
	t.Run("decode task still queued", func(t *testing.T) {
		r := &recordingTB{TB: t}
		verifyNoScanLeaks(r)
		var task decodeTask
		task.init(t.Context(), nil, nil, false, nil, nil)
		r.finish()
		require.True(t, task.cancel(errScanClosed))
		require.Len(t, r.errs, 1)
	})
	t.Run("decode worker still alive", func(t *testing.T) {
		r := &recordingTB{TB: t}
		verifyNoScanLeaks(r)
		p := newDecodePool(1, nil)
		r.finish()
		p.close()
		require.Len(t, r.errs, 1)
	})
}
