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

package logger

import "sync/atomic"

// initGeneration counts successful InitWithNative calls. A Lazy compares
// against it to know whether the logger it holds was built under the
// configuration now in force.
var initGeneration atomic.Uint64

// Lazy is a module logger built on first use and rebuilt after each
// InitWithNative.
//
// A package-level logger from GetLogger is built while its package is loaded,
// before InitWithNative runs. Named fixes a logger's writers when it builds the
// logger, so such a logger never gets the native destination that
// initialization configures, and its lines only reach the console. Lazy
// defers the build to the first call, and rebuilds when initialization has run
// since, so a call made before InitWithNative does not fix the wrong writers
// for good.
type Lazy struct {
	cur   atomic.Pointer[lazyLogger]
	scope []string
}

type lazyLogger struct {
	l   *Logger
	gen uint64
}

// NewLazy returns a Lazy for scope, which follows the rules of GetLogger.
func NewLazy(scope ...string) *Lazy {
	return &Lazy{scope: scope}
}

// Get returns the logger. After the first call it costs one atomic load and a
// comparison; it builds a logger only on first use and after initialization.
// Two callers can both build one at such a moment, and either result is
// correct.
func (z *Lazy) Get() *Logger {
	gen := initGeneration.Load()
	if cur := z.cur.Load(); cur != nil && cur.gen == gen {
		return cur.l
	}
	l := GetLogger(z.scope...)
	z.cur.Store(&lazyLogger{l: l, gen: gen})
	return l
}
