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

package consistency

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// Fingerprint hashes a schema object together with the index rules bound to it.
//
// Sorting and de-duplication of rules happen here rather than in
// DeriveBoundRules so the production derivation keeps its exact behavior while
// the fingerprint stays deterministic regardless of sync.Map iteration order.
func Fingerprint(subject proto.Message, rules []*databasev1.IndexRule) (uint64, error) {
	subjectHash, err := base(subject)
	if err != nil {
		return 0, err
	}
	ruleHashes := make(map[string]uint64, len(rules))
	keys := make([]string, 0, len(rules))
	for _, r := range rules {
		key := r.GetMetadata().GetGroup() + "/" + r.GetMetadata().GetName()
		if _, seen := ruleHashes[key]; seen {
			continue
		}
		ruleHash, ruleErr := base(r)
		if ruleErr != nil {
			return 0, ruleErr
		}
		ruleHashes[key] = ruleHash
		keys = append(keys, key)
	}
	sort.Strings(keys)

	digest := xxhash.New()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], subjectHash)
	_, _ = digest.Write(buf[:])
	for _, key := range keys {
		binary.LittleEndian.PutUint64(buf[:], ruleHashes[key])
		_, _ = digest.Write(buf[:])
	}
	return digest.Sum64(), nil
}

// base hashes one schema message after clearing the identity fields that move
// without any content change. The cleared set matches the IgnoreFields list in
// banyand/metadata/schema/checker.go.
//
// updated_at / created_at are deliberately NOT cleared: all three layers read
// the same stored value (registry reads and watch notifications share the same
// ToSchema conversion, and a no-op update never writes), so they cannot cause
// false positives -- and clearing more than necessary would mask real drift.
func base(m proto.Message) (uint64, error) {
	clone := proto.Clone(m)
	clearVolatile(clone)
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema for fingerprinting: %w", err)
	}
	return xxhash.Sum64(encoded), nil
}

// clearVolatile zeroes the identity fields on a cloned message. It operates on
// the clone made by base, never on the caller's live schema. Every BanyanDB
// schema protobuf (Group, Stream, Measure, Trace, IndexRule, IndexRuleBinding,
// TopNAggregation) carries a commonv1.Metadata, so the interface assertion
// covers them all without an exhaustive type switch.
func clearVolatile(m proto.Message) {
	type metadataHolder interface {
		GetMetadata() *commonv1.Metadata
	}
	h, ok := m.(metadataHolder)
	if !ok {
		return
	}
	md := h.GetMetadata()
	if md == nil {
		return
	}
	md.Id = 0
	md.CreateRevision = 0
	md.ModRevision = 0
}
