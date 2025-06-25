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

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type columnMetadata struct {
	name string
	dataBlock
	valueType pbv1.ValueType
}

func (cm *columnMetadata) reset() {
	cm.name = ""
	cm.valueType = 0
	cm.dataBlock.reset()
}

func (cm *columnMetadata) copyFrom(src *columnMetadata) {
	cm.name = src.name
	cm.valueType = src.valueType
	cm.dataBlock.copyFrom(&src.dataBlock)
}

func (cm *columnMetadata) marshal(dst []byte) []byte {
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(cm.name))
	dst = append(dst, byte(cm.valueType))
	dst = cm.dataBlock.marshal(dst)
	return dst
}

func (cm *columnMetadata) unmarshal(src []byte) ([]byte, error) {
	src, nameBytes, err := encoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal columnMetadata.name: %w", err)
	}
	cm.name = string(nameBytes)
	if len(src) < 1 {
		return nil, fmt.Errorf("cannot unmarshal columnMetadata.valueType: src is too short")
	}
	cm.valueType = pbv1.ValueType(src[0])
	src = src[1:]
	src = cm.dataBlock.unmarshal(src)
	return src, nil
}

type columnFamilyMetadata struct {
	columnMetadata []columnMetadata
}

func (cfm *columnFamilyMetadata) reset() {
	cms := cfm.columnMetadata
	for i := range cms {
		cms[i].reset()
	}
	cfm.columnMetadata = cms[:0]
}

func (cfm *columnFamilyMetadata) copyFrom(src *columnFamilyMetadata) {
	cfm.reset()
	cms := cfm.resizeColumnMetadata(len(src.columnMetadata))
	for i := range src.columnMetadata {
		cms[i].copyFrom(&src.columnMetadata[i])
	}
}

func (cfm *columnFamilyMetadata) resizeColumnMetadata(columnMetadataLen int) []columnMetadata {
	cms := cfm.columnMetadata
	if n := columnMetadataLen - cap(cms); n > 0 {
		cms = append(cms[:cap(cms)], make([]columnMetadata, n)...)
	}
	cms = cms[:columnMetadataLen]
	cfm.columnMetadata = cms
	return cms
}

func (cfm *columnFamilyMetadata) marshal(dst []byte) []byte {
	cms := cfm.columnMetadata
	dst = encoding.VarUint64ToBytes(dst, uint64(len(cms)))
	for i := range cms {
		dst = cms[i].marshal(dst)
	}
	return dst
}

func (cfm *columnFamilyMetadata) unmarshal(src []byte) ([]byte, error) {
	src, columnMetadataLen := encoding.BytesToVarUint64(src)
	if columnMetadataLen < 1 {
		return src, nil
	}
	cms := cfm.resizeColumnMetadata(int(columnMetadataLen))
	var err error
	for i := range cms {
		src, err = cms[i].unmarshal(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal columnMetadata %d: %w", i, err)
		}
	}
	return src, nil
}

func generateColumnFamilyMetadata() *columnFamilyMetadata {
	v := columnFamilyMetadataPool.Get()
	if v == nil {
		return &columnFamilyMetadata{}
	}
	return v
}

func releaseColumnFamilyMetadata(cfm *columnFamilyMetadata) {
	cfm.reset()
	columnFamilyMetadataPool.Put(cfm)
}

var columnFamilyMetadataPool = pool.Register[*columnFamilyMetadata]("measure-columnFamilyMetadata")
