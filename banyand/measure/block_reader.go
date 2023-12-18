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

package measure

import "github.com/apache/skywalking-banyandb/pkg/fs"

type reader struct {
	r         fs.Reader
	bytesRead uint64
}

func newReader(r fs.Reader) *reader {
	return &reader{r: r}
}

func (r *reader) reset() {
	r.r = nil
	r.bytesRead = 0
}

func (r *reader) Path() string {
	return r.r.Path()
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(0, p)
	r.bytesRead += uint64(n)
	return n, err
}