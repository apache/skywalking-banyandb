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
	"time"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	_          encoding.SeriesEncoderPool = (*encoderPool)(nil)
	_          encoding.SeriesDecoderPool = (*decoderPool)(nil)
	intervalFn                            = func(key []byte) time.Duration {
		_, interval, err := decodeFieldFlag(key)
		if err != nil {
			panic(err)
		}
		return interval
	}
)

type encoderPool struct {
	intPool     encoding.SeriesEncoderPool
	defaultPool encoding.SeriesEncoderPool
	l           *logger.Logger
}

func newEncoderPool(name string, plainSize, intSize int, l *logger.Logger) encoding.SeriesEncoderPool {
	return &encoderPool{
		intPool:     encoding.NewIntEncoderPool(name, intSize, intervalFn),
		defaultPool: encoding.NewPlainEncoderPool(name, plainSize),
		l:           l,
	}
}

func (p *encoderPool) Get(metadata []byte) encoding.SeriesEncoder {
	fieldSpec, _, err := decodeFieldFlag(metadata)
	if err != nil {
		p.l.Err(err).Msg("failed to decode field flag")
		return p.defaultPool.Get(metadata)
	}
	if fieldSpec.EncodingMethod == databasev1.EncodingMethod_ENCODING_METHOD_GORILLA {
		return p.intPool.Get(metadata)
	}
	return p.defaultPool.Get(metadata)
}

func (p *encoderPool) Put(encoder encoding.SeriesEncoder) {
	p.intPool.Put(encoder)
	p.defaultPool.Put(encoder)
}

type decoderPool struct {
	intPool     encoding.SeriesDecoderPool
	defaultPool encoding.SeriesDecoderPool
	l           *logger.Logger
}

func newDecoderPool(name string, plainSize, intSize int, l *logger.Logger) encoding.SeriesDecoderPool {
	return &decoderPool{
		intPool:     encoding.NewIntDecoderPool(name, intSize, intervalFn),
		defaultPool: encoding.NewPlainDecoderPool(name, plainSize),
		l:           l,
	}
}

func (p *decoderPool) Get(metadata []byte) encoding.SeriesDecoder {
	fieldSpec, _, err := decodeFieldFlag(metadata)
	if err != nil {
		p.l.Err(err).Msg("failed to decode field flag")
		return p.defaultPool.Get(metadata)
	}
	if fieldSpec.EncodingMethod == databasev1.EncodingMethod_ENCODING_METHOD_GORILLA {
		return p.intPool.Get(metadata)
	}
	return p.defaultPool.Get(metadata)
}

func (p *decoderPool) Put(decoder encoding.SeriesDecoder) {
	p.intPool.Put(decoder)
	p.defaultPool.Put(decoder)
}

const fieldFlagLength = 9

func EncoderFieldFlag(fieldSpec *databasev1.FieldSpec, interval time.Duration) []byte {
	encodingMethod := byte(fieldSpec.GetEncodingMethod().Number())
	compressionMethod := byte(fieldSpec.GetCompressionMethod().Number())
	bb := make([]byte, fieldFlagLength)
	bb[0] = encodingMethod<<4 | compressionMethod
	copy(bb[1:], convert.Int64ToBytes(int64(interval)))
	return bb
}

func decodeFieldFlag(key []byte) (*databasev1.FieldSpec, time.Duration, error) {
	if len(key) < fieldFlagLength {
		return nil, 0, ErrMalformedFieldFlag
	}
	b := key[len(key)-9:]
	return &databasev1.FieldSpec{
		EncodingMethod:    databasev1.EncodingMethod(int32(b[0]) >> 4),
		CompressionMethod: databasev1.CompressionMethod((int32(b[0] & 0x0F))),
	}, time.Duration(convert.BytesToInt64(b[1:])), nil
}
