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
package helpers

import (
	"sync/atomic"

	"github.com/onsi/gomega/types"
)

type FailInterceptor struct {
	ginkgoFail types.GomegaFailHandler
	didFail    *atomic.Bool
}

func NewFailInterceptor(fail types.GomegaFailHandler) *FailInterceptor {
	return &FailInterceptor{
		ginkgoFail: fail,
		didFail:    &atomic.Bool{},
	}
}

func (f *FailInterceptor) Fail(message string, callerSkip ...int) {
	f.didFail.Store(true)
	if len(callerSkip) == 0 {
		f.ginkgoFail(message, 1)
	} else {
		f.ginkgoFail(message, callerSkip[0]+1)
	}
}

func (f *FailInterceptor) Reset() {
	f.didFail.Store(false)
}

func (f *FailInterceptor) DidFail() bool {
	return f.didFail.Load()
}
