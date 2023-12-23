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
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
)

func (tst *tsTable) currentSnapshot() *snapshot {
	tst.RLock()
	defer tst.RUnlock()
	if tst.snapshot == nil {
		return nil
	}
	s := tst.snapshot
	s.incRef()
	return s
}

type snapshot struct {
	parts []*partWrapper
	epoch uint64

	ref int32
}

func (s *snapshot) getParts(dst []*part, opts queryOptions) ([]*part, int) {
	var count int
	for _, p := range s.parts {
		pm := p.p.partMetadata
		if opts.maxTimestamp < pm.MinTimestamp || opts.minTimestamp > pm.MaxTimestamp {
			continue
		}
		dst = append(dst, p.p)
		count++
	}
	return dst, count
}

func (s *snapshot) incRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *snapshot) decRef() {
	n := atomic.AddInt32(&s.ref, -1)
	if n > 0 {
		return
	}
	for i := range s.parts {
		s.parts[i].decRef()
	}
	s.parts = s.parts[:0]
}

func (s snapshot) copyAllTo(nextEpoch uint64) snapshot {
	s.epoch = nextEpoch
	s.ref = 1
	for i := range s.parts {
		s.parts[i].incRef()
	}
	return s
}

func (s *snapshot) merge(nextEpoch uint64, nextParts map[uint64]*partWrapper) snapshot {
	var result snapshot
	result.epoch = nextEpoch
	result.ref = 1
	for i := 0; i < len(s.parts); i++ {
		if n, ok := nextParts[s.parts[i].ID()]; ok {
			result.parts = append(result.parts, n)
			continue
		}
		s.parts[i].incRef()
		result.parts = append(result.parts, s.parts[i])
	}
	return result
}

func snapshotName(snapshot uint64) string {
	return fmt.Sprintf("%016x%s", snapshot, snapshotSuffix)
}

func parseSnapshot(name string) (uint64, error) {
	if filepath.Ext(name) != snapshotSuffix {
		return 0, errors.New("invalid snapshot file ext")
	}
	if len(name) < 16 {
		return 0, errors.New("invalid snapshot file name")
	}
	return parseEpoch(name[:16])
}
