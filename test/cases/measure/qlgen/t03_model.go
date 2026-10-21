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

package qlgen

// T03Column is an independently named reference in a generated raw SELECT.
type T03Column struct {
	Source string
	Alias  string
}

// T03Time is a pure time predicate over the storage timestamp lattice.
type T03Time struct {
	Absolute string
	Op       string
	Children []*T03Time
	Nanos    int64
	Now      bool
}

// T03Query models extended raw SQL independently of production syntax trees.
type T03Query struct {
	Where           *T03Time
	LegacyBegin     *int64
	Limit           *uint32
	ClockMillis     *int64
	OrderBy         string
	Columns         []T03Column
	LegacyEnd       int64
	Offset          uint32
	Bind            bool
	TimestampParams bool
	Shorthand       bool
	Descending      bool
	Trace           bool
	NoScan          bool
	Legacy          bool
}

func t03RID() T03Column                           { return T03Column{Source: "rid"} }
func t03Compare(op string, millis int64) *T03Time { return &T03Time{Op: op, Nanos: millis * 1_000_000} }
func t03And(children ...*T03Time) *T03Time        { return &T03Time{Op: "AND", Children: children} }
func t03Window() *T03Time                         { return t03And(t03Compare(">=", 0), t03Compare("<=", 7000)) }
