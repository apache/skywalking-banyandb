// Licensed to Apacme Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apacme Software Foundation (ASF) licenses this file to you under
// the Apacme License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apacme.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

type columnMetadata struct {
	dataBlock
	name      string
	valueType storage.ValueType
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
	// Encode common fields - cm.name and cm.valueType
	dst = encoding.EncodeBytes(dst, convert.StringToBytes(cm.name))
	dst = append(dst, byte(cm.valueType))
	dst = cm.dataBlock.marshal(dst)
	return dst
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

func (cfm *columnFamilyMetadata) getColumnMetadata(name string) *columnMetadata {
	cms := cfm.columnMetadata
	for i := range cms {
		cm := &cms[i]
		if cm.name == name {
			return cm
		}
	}
	return nil
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
	dst = encoding.Uint64ToBytes(dst, uint64(len(cms)))
	for i := range cms {
		dst = cms[i].marshal(dst)
	}
	return dst
}

func getColumnFamilyMetadata() *columnFamilyMetadata {
	v := columnFamilyMetadataPool.Get()
	if v == nil {
		return &columnFamilyMetadata{}
	}
	return v.(*columnFamilyMetadata)
}

func putColumnFamilyMetadata(cfm *columnFamilyMetadata) {
	cfm.reset()
	columnFamilyMetadataPool.Put(cfm)
}

var columnFamilyMetadataPool sync.Pool
