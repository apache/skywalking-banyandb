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
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/pb"
	posting2 "github.com/apache/skywalking-banyandb/pkg/posting"
)

const (
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

func (s *service) FetchTrace(traceSeries common.Metadata, traceID string, opt series.ScanOptions) (data.Trace, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return data.Trace{}, err
	}
	return ts.FetchTrace(traceID, opt)
}

func (s *service) FetchEntity(traceSeries common.Metadata, shardID uint, chunkIDs posting2.List, opt series.ScanOptions) ([]data.Entity, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return nil, err
	}
	return ts.FetchEntity(chunkIDs, shardID, opt)
}

func (s *service) ScanEntity(traceSeries common.Metadata, startTime, endTime uint64, opt series.ScanOptions) ([]data.Entity, error) {
	ts, err := s.getSeries(traceSeries)
	if err != nil {
		return nil, err
	}
	return ts.ScanEntity(startTime, endTime, opt)
}

func (s *service) getSeries(traceSeries common.Metadata) (*traceSeries, error) {
	id := formatTraceSeriesID(traceSeries.Spec.GetName(), traceSeries.Spec.GetGroup())
	s.l.Debug().Str("id", id).Msg("got Trace series")
	ts, ok := s.schemaMap[id]
	if !ok {
		return nil, errors.Wrapf(ErrTraceSeriesNotFound, "series id:%s, map:%v", id, s.schemaMap)
	}
	return ts, nil
}

func (s *service) Write(traceSeriesMetadata common.Metadata, ts time.Time, seriesID, entityID string, dataBinary []byte, items ...interface{}) (bool, error) {
	traceSeries, err := s.getSeries(traceSeriesMetadata)
	if err != nil {
		return false, err
	}

	ev := pb.NewEntityValueBuilder().
		DataBinary(dataBinary).
		EntityID(entityID).
		Fields(items...).
		Timestamp(ts).
		Build()

	seriesIDBytes := []byte(seriesID)
	shardID, shardIDError := partition.ShardID(seriesIDBytes, traceSeries.shardNum)
	if shardIDError != nil {
		return err == nil, shardIDError
	}
	_, err = traceSeries.Write(common.SeriesID(convert.Hash(seriesIDBytes)), shardID, data.EntityValue{
		EntityValue: ev,
	})
	return err == nil, err
}

func formatTraceSeriesID(name, group string) string {
	return name + ":" + group
}

type traceSeries struct {
	name                         string
	group                        string
	idGen                        series.IDGen
	l                            *logger.Logger
	schema                       apischema.TraceSeries
	reader                       storage.StoreRepo
	writePoint                   storage.GetWritePoint
	idx                          index.Service
	shardNum                     uint32
	fieldIndex                   map[string]*fieldSpec
	traceIDIndex                 int
	traceIDFieldName             string
	stateFieldName               string
	stateFieldType               databasev1.FieldType
	strStateSuccessVal           string
	strStateErrorVal             string
	intStateSuccessVal           int64
	intStateErrorVal             int64
	stateIndex                   int
	fieldsNamesCompositeSeriesID []string
}

type fieldSpec struct {
	idx  int
	spec *databasev1.FieldSpec
}

func newTraceSeries(schema apischema.TraceSeries, l *logger.Logger, idx index.Service) (*traceSeries, error) {
	t := &traceSeries{
		schema: schema,
		idGen:  series.NewIDGen(),
		l:      l,
		idx:    idx,
	}
	meta := t.schema.Spec.GetMetadata()
	shardInfo := t.schema.Spec.GetShard()
	t.shardNum = shardInfo.GetNumber()
	t.name, t.group = meta.GetName(), meta.GetGroup()
	if err := t.buildFieldIndex(); err != nil {
		return nil, err
	}
	traceID, ok := t.fieldIndex[t.traceIDFieldName]
	if !ok {
		return nil, errors.Wrapf(ErrFieldSchemaNotFound, "trace_id field name:%s\n field index:%v",
			t.traceIDFieldName, t.fieldIndex)
	}
	t.traceIDIndex = traceID.idx
	state, ok := t.fieldIndex[t.stateFieldName]
	if !ok {
		return nil, errors.Wrapf(ErrFieldSchemaNotFound, "state field name:%s\n field index:%v",
			t.traceIDFieldName, t.fieldIndex)
	}
	t.stateIndex = state.idx
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
	reservedMap := spec.GetReservedFieldsMap()
	t.traceIDFieldName = reservedMap.GetTraceId()
	state := reservedMap.GetState()
	stateFieldName := state.GetField()

	fieldsLen := len(spec.GetFields())
	t.fieldIndex = make(map[string]*fieldSpec, fieldsLen)
	for idx, f := range spec.GetFields() {
		if f.GetName() == stateFieldName {
			t.stateFieldType = f.GetType()
		}
		t.fieldIndex[f.GetName()] = &fieldSpec{
			idx:  idx,
			spec: f,
		}
	}
	switch t.stateFieldType {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		t.strStateSuccessVal = state.GetValSuccess()
		t.strStateErrorVal = state.GetValError()
	case databasev1.FieldType_FIELD_TYPE_INT:
		intSVal, err := strconv.ParseInt(state.GetValSuccess(), 10, 64)
		if err != nil {
			return err
		}
		t.intStateSuccessVal = intSVal
		intEVal, err := strconv.ParseInt(state.GetValError(), 10, 64)
		if err != nil {
			return err
		}
		t.intStateErrorVal = intEVal
	default:
		return errors.Wrapf(ErrUnsupportedFieldType, "type:%s, supported type: Int and String", t.stateFieldType.String())
	}
	t.stateFieldName = stateFieldName

	t.fieldsNamesCompositeSeriesID = make([]string, 0, len(reservedMap.GetSeriesId()))
	for i := 0; i < len(reservedMap.GetSeriesId()); i++ {
		t.fieldsNamesCompositeSeriesID = append(t.fieldsNamesCompositeSeriesID, reservedMap.GetSeriesId()[i])
	}

	return nil
}

// getTraceID extracts traceID as bytes from v1.EntityValue
func (t *traceSeries) getTraceID(entityValue *tracev1.EntityValue) ([]byte, error) {
	if entityValue.GetFields() == nil {
		return nil, errors.Wrapf(ErrFieldNotFound, "EntityValue does not contain any fields")
	}
	if len(entityValue.GetFields()) < t.traceIDIndex+1 {
		return nil, errors.Wrapf(ErrFieldNotFound, "EntityValue contains incomplete fields")
	}
	f := entityValue.GetFields()[t.traceIDIndex]
	if f == nil {
		return nil, errors.Wrapf(ErrFieldNotFound, "trace_id index %d", t.traceIDIndex)
	}
	switch v := f.GetValueType().(type) {
	case *modelv1.Field_Str:
		return []byte(v.Str.GetValue()), nil
	default:
		// TODO: add a test to cover the default case
		return nil, errors.Wrapf(ErrUnsupportedFieldType, "type: %v, supported type: String", v)
	}
}

func (t *traceSeries) getState(entityValue *tracev1.EntityValue) (state State, fieldStoreName, dataStoreName string, err error) {
	if entityValue.GetFields() == nil {
		err = errors.Wrapf(ErrFieldNotFound, "EntityValue does not contain any fields")
		return
	}
	if len(entityValue.GetFields()) < t.stateIndex+1 {
		err = errors.Wrapf(ErrFieldNotFound, "EntityValue contains incomplete fields")
		return
	}

	f := entityValue.GetFields()[t.stateIndex]
	if f == nil {
		err = errors.Wrapf(ErrFieldNotFound, "state index %d", t.stateIndex)
		return
	}

	switch v := f.GetValueType().(type) {
	case *modelv1.Field_Int:
		if t.stateFieldType != databasev1.FieldType_FIELD_TYPE_INT {
			// TODO: add a test case to cover this line
			err = errors.Wrapf(ErrUnsupportedFieldType, "given type: Int, supported type: %s", t.stateFieldType.String())
			return
		}
		switch v.Int.GetValue() {
		case t.intStateSuccessVal:
			state = StateSuccess
		case t.intStateErrorVal:
			state = StateError
		default:
			err = errors.Wrapf(ErrUnknownFieldValue, "value:%d, supported value: %d, %d",
				v.Int.GetValue(), t.intStateSuccessVal, t.intStateErrorVal)
			return
		}
	case *modelv1.Field_Str:
		if t.stateFieldType != databasev1.FieldType_FIELD_TYPE_STRING {
			err = errors.Wrapf(ErrUnsupportedFieldType, "given type: String, supported type: %s", t.stateFieldType.String())
			return
		}
		switch v.Str.GetValue() {
		case t.strStateSuccessVal:
			state = StateSuccess
		case t.strStateErrorVal:
			state = StateError
		default:
			err = errors.Wrapf(ErrUnknownFieldValue, "value:%s, supported value: %s, %s",
				v.Str.GetValue(), t.strStateSuccessVal, t.strStateErrorVal)
			return
		}
	default:
		// TODO: cover?
		err = errors.Wrapf(ErrUnsupportedFieldType, "type: %s, supported type: String and Int", v)
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
