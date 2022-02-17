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
//
package bucket_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
)

type queueEntryID struct {
	first  uint16
	second uint16
}

func entryID(id uint16) queueEntryID {
	return queueEntryID{
		first:  id,
		second: id + 1,
	}
}

var _ = Describe("Queue", func() {
	It("pushes data", func() {
		evictLst := make([]queueEntryID, 0)
		l, err := bucket.NewQueue(128, func(id interface{}) {
			evictLst = append(evictLst, id.(queueEntryID))
		})
		Expect(err).ShouldNot(HaveOccurred())

		for i := 0; i < 256; i++ {
			l.Push(entryID(uint16(i)))
		}
		Expect(l.Len()).To(Equal(128))
		Expect(len(evictLst)).To(Equal(64))
		for i := 0; i < 64; i++ {
			Expect(evictLst[i]).To(Equal(entryID(uint16(i))))
		}
	})
})
