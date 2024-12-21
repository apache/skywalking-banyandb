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

package stream

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type tagMetadata struct {
	name string
	dataBlock
	valueType pbv1.ValueType
}

func (tm *tagMetadata) reset() {
	tm.name = ""
	tm.valueType = 0
	tm.dataBlock.reset()
}

func (tm *tagMetadata) copyFrom(src *tagMetadata) {
	tm.name = src.name
	tm.valueType = src.valueType
	tm.dataBlock.copyFrom(&src.dataBlock)
}

func (tm *tagMetadata) marshal(dst []byte) []byte {
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(tm.name))
	dst = append(dst, byte(tm.valueType))
	dst = tm.dataBlock.marshal(dst)
	return dst
}

func (tm *tagMetadata) unmarshal(src []byte) ([]byte, error) {
	src, nameBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.name: %w", err)
	}
	tm.name = string(nameBytes)
	if len(src) < 1 {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.valueType: src is too short")
	}
	tm.valueType = pbv1.ValueType(src[0])
	src = src[1:]
	src = tm.dataBlock.unmarshal(src)
	return src, nil
}

type tagFamilyMetadata struct {
	tagMetadata []tagMetadata
}

func (tfm *tagFamilyMetadata) reset() {
	tms := tfm.tagMetadata
	for i := range tms {
		tms[i].reset()
	}
	tfm.tagMetadata = tms[:0]
}

func (tfm *tagFamilyMetadata) copyFrom(src *tagFamilyMetadata) {
	tfm.reset()
	tms := tfm.resizeTagMetadata(len(src.tagMetadata))
	for i := range src.tagMetadata {
		tms[i].copyFrom(&src.tagMetadata[i])
	}
}

func (tfm *tagFamilyMetadata) resizeTagMetadata(tagMetadataLen int) []tagMetadata {
	tms := tfm.tagMetadata
	if n := tagMetadataLen - cap(tms); n > 0 {
		tms = append(tms[:cap(tms)], make([]tagMetadata, n)...)
	}
	tms = tms[:tagMetadataLen]
	tfm.tagMetadata = tms
	return tms
}

func (tfm *tagFamilyMetadata) marshal(dst []byte) []byte {
	tms := tfm.tagMetadata
	dst = encoding.VarUint64ToBytes(dst, uint64(len(tms)))
	for i := range tms {
		dst = tms[i].marshal(dst)
	}
	return dst
}

func (tfm *tagFamilyMetadata) unmarshal(src []byte) error {
	src, tagMetadataLen := encoding.BytesToVarUint64(src)
	if tagMetadataLen < 1 {
		return nil
	}
	tms := tfm.resizeTagMetadata(int(tagMetadataLen))
	var err error
	for i := range tms {
		src, err = tms[i].unmarshal(src)
		if err != nil {
			return fmt.Errorf("cannot unmarshal tagMetadata %d: %w", i, err)
		}
	}
	return nil
}

func generateTagFamilyMetadata() *tagFamilyMetadata {
	v := tagFamilyMetadataPool.Get()
	if v == nil {
		return &tagFamilyMetadata{}
	}
	return v
}

func releaseTagFamilyMetadata(tfm *tagFamilyMetadata) {
	tfm.reset()
	tagFamilyMetadataPool.Put(tfm)
}

var tagFamilyMetadataPool = pool.Register[*tagFamilyMetadata]("stream-tagFamilyMetadata")
