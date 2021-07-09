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

package trace

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	traceSeriesIDTemp = "%s:%s"
	// KV stores
	chunkIDMapping = "chunkIDMapping"
	startTimeIndex = "startTimeIndex"
	// Time series stores
	successDataStore   = "successDataStore"
	successFieldsStore = "successFieldsStore"
	errorDataStore     = "errorDataStore"
	errorFieldsStore   = "errorFieldsStore"

	traceIndex = "traceIndex"
)

var (
	_                       storage.Plugin   = (*traceSeries)(nil)
	_                       series.TraceRepo = (*service)(nil)
	ErrTraceSeriesNotFound                   = errors.New("failed to find Trace series")
	ErrFieldSchemaNotFound                   = errors.New("failed to find trace_id specification")
	ErrFieldNotFound                         = errors.New("failed to find specific field")
	ErrProjectionEmpty                       = errors.New("projection is empty")
	ErrChunkIDsEmpty                         = errors.New("chunkID is empty")
	ErrInvalidTraceID                        = errors.New("invalid Trace id")
	ErrUnsupportedFieldType                  = errors.New("unsupported field type")
	ErrUnknownFieldValue                     = errors.New("unknown field value")
	ErrInvalidKey                            = errors.New("invalid key")
	ErrUnknownState                          = errors.New("unknown state value")
)

type State byte

const (
	StateSuccess = 0
	StateError   = 1
)

var _ series.Service = (*service)(nil)

type service struct {
	db        storage.Database
	schemaMap map[string]*traceSeries
	l         *logger.Logger
}

//NewService returns a new service
func NewService(_ context.Context, db storage.Database) (series.Service, error) {
	return &service{db: db}, nil
}

func (s *service) Name() string {
	return "trace-series"
}

func (s *service) PreRun() error {
	schemas, err := s.TraceSeries().List(context.Background(), schema.ListOpt{})
	if err != nil {
		return err
	}
	s.schemaMap = make(map[string]*traceSeries, len(schemas))
	s.l = logger.GetLogger(s.Name())
	for _, sa := range schemas {
		ts, errTS := newTraceSeries(sa, s.l)
		if errTS != nil {
			return errTS
		}
		s.db.Register(ts)
		id := fmt.Sprintf(traceSeriesIDTemp, ts.name, ts.group)
		s.schemaMap[id] = ts
		s.l.Info().Str("id", id).Msg("initialize Trace series")
	}
	return err
}

func (s *service) FetchTrace(traceSeries common.Metadata, traceID string, opt series.ScanOptions) (data.Trace, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return data.Trace{}, err
	}
	return ts.FetchTrace(traceID, opt)
}

func (s *service) FetchEntity(traceSeries common.Metadata, chunkIDs []common.ChunkID, opt series.ScanOptions) ([]data.Entity, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return nil, err
	}
	return ts.FetchEntity(chunkIDs, opt)
}

func (s *service) ScanEntity(traceSeries common.Metadata, startTime, endTime uint64, opt series.ScanOptions) ([]data.Entity, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return nil, err
	}
	return ts.ScanEntity(startTime, endTime, opt)
}

func (s *service) getSeries(traceSeries common.Metadata) (*traceSeries, error) {
	id := getTraceSeriesID(traceSeries)
	s.l.Debug().Str("id", id).Msg("got Trace series")
	ts, ok := s.schemaMap[id]
	if !ok {
		return nil, errors.Wrapf(ErrTraceSeriesNotFound, "series id:%s, map:%v", id, s.schemaMap)
	}
	return ts, nil
}

func getTraceSeriesID(traceSeries common.Metadata) string {
	return fmt.Sprintf(traceSeriesIDTemp, string(traceSeries.Spec.Name()), string(traceSeries.Spec.Group()))
}

type traceSeries struct {
	name               string
	group              string
	idGen              series.IDGen
	l                  *logger.Logger
	schema             apischema.TraceSeries
	reader             storage.StoreRepo
	writePoint         storage.GetWritePoint
	shardNum           uint
	fieldIndex         map[string]uint
	traceIDIndex       uint
	traceIDFieldName   string
	stateFieldName     string
	stateFieldType     v1.FieldType
	strStateSuccessVal string
	strStateErrorVal   string
	intStateSuccessVal int64
	intStateErrorVal   int64
	stateIndex         uint
}

func newTraceSeries(schema apischema.TraceSeries, l *logger.Logger) (*traceSeries, error) {
	t := &traceSeries{
		schema: schema,
		idGen:  series.NewIDGen(),
		l:      l,
	}
	meta := t.schema.Spec.Metadata(nil)
	shardInfo := t.schema.Spec.Shard(nil)
	t.shardNum = uint(shardInfo.Number())
	t.name = string(meta.Name())
	t.group = string(meta.Group())
	if err := t.buildFieldIndex(); err != nil {
		return nil, err
	}
	traceID, ok := t.fieldIndex[t.traceIDFieldName]
	if !ok {
		return nil, errors.Wrapf(ErrFieldSchemaNotFound, "trace_id field name:%s\n field index:%v",
			t.traceIDFieldName, t.fieldIndex)
	}
	t.traceIDIndex = traceID
	state, ok := t.fieldIndex[t.stateFieldName]
	if !ok {
		return nil, errors.Wrapf(ErrFieldSchemaNotFound, "state field name:%s\n field index:%v",
			t.traceIDFieldName, t.fieldIndex)
	}
	t.stateIndex = state
	return t, nil
}

func (t *traceSeries) Meta() storage.PluginMeta {
	return storage.PluginMeta{
		ID:          t.name,
		Group:       t.group,
		ShardNumber: t.shardNum,
		KVSpecs: []storage.KVSpec{
			{
				Name: chunkIDMapping,
				Type: storage.KVTypeNormal,
			},
			{
				Name: startTimeIndex,
				Type: storage.KVTypeNormal,
			},

			{
				Name:          successDataStore,
				Type:          storage.KVTypeTimeSeries,
				CompressLevel: 3,
			},
			{
				Name:          successFieldsStore,
				Type:          storage.KVTypeTimeSeries,
				CompressLevel: 3,
			},
			{
				Name:          errorDataStore,
				Type:          storage.KVTypeTimeSeries,
				CompressLevel: 3,
			},
			{
				Name:          errorFieldsStore,
				Type:          storage.KVTypeTimeSeries,
				CompressLevel: 3,
			},
			{
				Name:          traceIndex,
				Type:          storage.KVTypeTimeSeries,
				CompressLevel: -1,
			},
		},
	}
}

func (t *traceSeries) Init(repo storage.StoreRepo, point storage.GetWritePoint) {
	t.reader = repo
	t.writePoint = point
}

func (t *traceSeries) buildFieldIndex() error {
	spec := t.schema.Spec
	reservedMap := spec.ReservedFieldsMap(nil)
	t.traceIDFieldName = string(reservedMap.TraceId())
	state := reservedMap.State(nil)
	stateFieldName := state.Field()

	fieldsLen := spec.FieldsLength()
	index := make(map[string]uint, fieldsLen)
	for i := 0; i < fieldsLen; i++ {
		fieldSpec := new(v1.FieldSpec)
		if !spec.Fields(fieldSpec, i) {
			continue
		}
		if bytes.Equal(fieldSpec.Name(), stateFieldName) {
			t.stateFieldType = fieldSpec.Type()
		}
		index[string(fieldSpec.Name())] = uint(i)
	}
	switch t.stateFieldType {
	case v1.FieldTypeString:
		t.strStateSuccessVal = string(state.ValSuccess())
		t.strStateErrorVal = string(state.ValError())
	case v1.FieldTypeInt:
		intSVal, err := strconv.ParseInt(string(state.ValSuccess()), 10, 64)
		if err != nil {
			return err
		}
		t.intStateSuccessVal = intSVal
		intEVal, err := strconv.ParseInt(string(state.ValError()), 10, 64)
		if err != nil {
			return err
		}
		t.intStateErrorVal = intEVal
	default:
		return errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: Int and String", t.stateFieldType.String())
	}
	t.stateFieldName = string(stateFieldName)

	t.fieldIndex = index
	return nil
}

func (t *traceSeries) getTraceID(entityValue *v1.EntityValue) ([]byte, error) {
	f := new(v1.Field)
	if !entityValue.Fields(f, int(t.traceIDIndex)) {
		return nil, errors.Wrapf(ErrFieldNotFound, "trace_id index :%d", t.traceIDIndex)
	}
	if f.ValueType() != v1.ValueTypeString {
		return nil, errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: String", f.ValueType().String())
	}
	unionTable := new(flatbuffers.Table)
	f.Value(unionTable)
	stringValue := new(v1.String)
	stringValue.Init(unionTable.Bytes, unionTable.Pos)
	return stringValue.Value(), nil
}

func (t *traceSeries) getState(entityValue *v1.EntityValue) (state State, fieldStoreName, dataStoreName string, err error) {
	f := new(v1.Field)
	if !entityValue.Fields(f, int(t.stateIndex)) {
		err = errors.Wrapf(ErrFieldNotFound, "state index :%d", t.traceIDIndex)
		return
	}
	switch f.ValueType() {
	case v1.ValueTypeInt:
		if t.stateFieldType != v1.FieldTypeInt {
			err = errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: Int", f.ValueType().String())
			return
		}
		unionTable := new(flatbuffers.Table)
		f.Value(unionTable)
		intValue := new(v1.Int)
		intValue.Init(unionTable.Bytes, unionTable.Pos)
		switch intValue.Value() {
		case t.intStateSuccessVal:
			state = StateSuccess
		case t.intStateErrorVal:
			state = StateError
		default:
			err = errors.Wrapf(ErrUnknownFieldValue, "value:%d, supported value: %d, %d",
				intValue.Value(), t.intStateSuccessVal, t.intStateErrorVal)
			return
		}
	case v1.ValueTypeString:
		if t.stateFieldType != v1.FieldTypeString {
			err = errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: String", f.ValueType().String())
			return
		}
		unionTable := new(flatbuffers.Table)
		f.Value(unionTable)
		stringValue := new(v1.String)
		stringValue.Init(unionTable.Bytes, unionTable.Pos)
		switch string(stringValue.Value()) {
		case t.strStateSuccessVal:
			state = StateSuccess
		case t.strStateErrorVal:
			state = StateError
		default:
			err = errors.Wrapf(ErrUnknownFieldValue, "value:%s, supported value: %s, %s",
				string(stringValue.Value()), t.strStateSuccessVal, t.strStateErrorVal)
			return
		}
	default:
		err = errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: String and Int", f.ValueType().String())
		return
	}
	fieldStoreName, dataStoreName, err = getStoreName(state)
	return
}

func getStoreName(state State) (string, string, error) {
	switch state {
	case StateSuccess:
		return successFieldsStore, successDataStore, nil
	case StateError:
		return errorFieldsStore, errorDataStore, nil
	}
	return "", "", ErrUnknownState
}
