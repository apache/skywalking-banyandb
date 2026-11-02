// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pub

import (
	"errors"
	"testing"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
)

func TestRemoteErrorKeepsResourceExhaustedIdentity(t *testing.T) {
	exhausted := remoteError(modelv1.Status_STATUS_RESOURCE_EXHAUSTED, "code: STATUS_RESOURCE_EXHAUSTED, msg: pool exhausted")
	if !errors.Is(exhausted, protector.ErrQueryResourceExhausted) {
		t.Fatal("a resource-exhausted status must match protector.ErrQueryResourceExhausted")
	}
	if exhausted.Error() != "code: STATUS_RESOURCE_EXHAUSTED, msg: pool exhausted" {
		t.Fatalf("the remote message must be kept verbatim: %q", exhausted.Error())
	}
	for _, st := range []modelv1.Status{modelv1.Status_STATUS_UNSPECIFIED, modelv1.Status_STATUS_INTERNAL_ERROR} {
		if errors.Is(remoteError(st, "boom"), protector.ErrQueryResourceExhausted) {
			t.Fatalf("status %v must not be reported as resource exhaustion", st)
		}
	}
}
