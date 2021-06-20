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

package grpc

import (
	"context"
	"net"
	"strconv"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/encoding"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type Server struct {
	addr     string
	log      *logger.Logger
	ser      *grpclib.Server
	pipeline queue.Queue
}

func NewServer(ctx context.Context, pipeline queue.Queue) *Server {
	return &Server{pipeline: pipeline}
}

func (s *Server) Name() string {
	return "grpc"
}

func (s *Server) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("grpc")
	fs.StringVarP(&s.addr, "addr", "", ":17912", "the address of banyand listens")
	return fs
}

func (s *Server) Validate() error {
	return nil
}

func (s *Server) Serve() error {
	s.log = logger.GetLogger("grpc")
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Fatal("Failed to listen", logger.Error(err))
	}

	encoding.RegisterCodec(flatbuffers.FlatbuffersCodec{})
	s.ser = grpclib.NewServer()

	return s.ser.Serve(lis)
}

func (s *Server) GracefulStop() {
	s.log.Info("stopping")
	s.ser.GracefulStop()
}

func (s *Server) WriteTraces(entityValue *v1.EntityValue, metaData *v1.Metadata) (*flatbuffers.Builder, error) {
	s.log.Info("WriteTraces called...")
	builder := flatbuffers.NewBuilder(0)
	// Serialize MetaData
	group, name := builder.CreateString(string(metaData.Group())), builder.CreateString(string(metaData.Name()))
	v1.MetadataStart(builder)
	v1.MetadataAddGroup(builder, group)
	v1.MetadataAddName(builder, name)
	v1.MetadataEnd(builder)
	// Serialize Fields
	v1.FieldStart(builder)
	var fieldList []flatbuffers.UOffsetT
	for i := 0; i < entityValue.FieldsLength(); i++ {
		var f v1.Field
		var s string
		if ok := entityValue.Fields(&f, i); ok {
			unionValueType := new(flatbuffers.Table)
			if f.Value(unionValueType) {
				valueType := f.ValueType()
				if valueType == v1.ValueTypeString {
					unionStr := new(v1.String)
					unionStr.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldAddValueType(builder, v1.ValueTypeString)
					s = string(unionStr.Value())
				} else if valueType == v1.ValueTypeInt {
					unionInt := new(v1.Int)
					unionInt.Init(unionValueType.Bytes, unionValueType.Pos)
					v1.FieldAddValueType(builder, v1.ValueTypeInt)
					field := flatbuffers.UOffsetT(unionInt.Value())
					v1.FieldAddValue(builder, field)
					fieldList = append(fieldList, field)
				} else if valueType == v1.ValueTypeStringArray {
					unionStrArray := new(v1.StringArray)
					unionStrArray.Init(unionValueType.Bytes, unionValueType.Pos)
					len := unionStrArray.ValueLength()
					if len == 1 {
						s += string(unionStrArray.Value(0))
					} else {
						s += "["
						for i := 0; i < len; i++ {
							s += string(unionStrArray.Value(i))
						}
						s += "]"
					}
				} else if valueType == v1.ValueTypeIntArray {
					unionIntArray := new(v1.IntArray)
					unionIntArray.Init(unionValueType.Bytes, unionValueType.Pos)
					l := unionIntArray.ValueLength()
					if l == 1 {
						s += strconv.FormatInt(unionIntArray.Value(0), 10)
					} else if l > 1{
						s += "["
						for i := 0; i < l; i++ {
							s += strconv.FormatInt(unionIntArray.Value(i), 10)
						}
						s += "]"
					}
				}
				if valueType == v1.ValueTypeIntArray || valueType == v1.ValueTypeStringArray || valueType == v1.ValueTypeString {
					field := builder.CreateString(s)
					v1.FieldAddValue(builder, field)
					fieldList = append(fieldList, field)
				}
			}
		}
	}
	v1.FieldEnd(builder)
	// Serialize EntityValue
	dataBinaryLength := entityValue.DataBinaryLength()
	v1.EntityStartDataBinaryVector(builder, dataBinaryLength)
	for i := dataBinaryLength; i >= 0; i-- {
		builder.PrependByte(byte(i))
	}
	dataBinary := builder.EndVector(dataBinaryLength)
	entityId := builder.CreateString(string(entityValue.EntityId()))
	v1.EntityValueStart(builder)
	v1.EntityValueAddEntityId(builder, entityId)
	time := uint64(time.Now().UnixNano())
	v1.EntityValueAddTimestampNanoseconds(builder, time)
	v1.EntityValueAddDataBinary(builder, dataBinary)
	v1.EntityValueStartFieldsVector(builder, len(fieldList))
	for val := range fieldList {
		builder.PrependUOffsetT(flatbuffers.UOffsetT(val))
	}
	fields := builder.EndVector(len(fieldList))
	v1.EntityValueAddFields(builder, fields)
	v1.EntityValueEnd(builder)
	trace := v1.WriteEntityEnd(builder)

	builder.Finish(trace)

	return builder, nil
}

func (s *Server) FetchTraces(in *v1.EntityCriteria) (*flatbuffers.Builder, error) {
	s.log.Info("Fetch called...")
	builder := flatbuffers.NewBuilder(0)
	//buf := builder.FinishedBytes()
	//entityCriteria := v1.GetRootAsEntityCriteria(buf, 0)

	return builder, nil
}
