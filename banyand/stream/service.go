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

package stream

import (
	"context"

	"github.com/pkg/errors"

	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	ErrEmptyRootPath  = errors.New("root path is empty")
	ErrStreamNotExist = errors.New("stream doesn't exist")
)

type Service interface {
	run.PreRunner
	run.Config
	run.Service
	Query
}

var _ Service = (*service)(nil)

type service struct {
	schemaMap     map[string]*stream
	writeListener *writeCallback
	l             *logger.Logger
	metadata      metadata.Repo
	root          string
}

func (s *service) Stream(stream *commonv2.Metadata) (Stream, error) {
	sID := formatStreamID(stream.GetName(), stream.GetGroup())
	sm, ok := s.schemaMap[sID]
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (s *service) StreamT(stream *commonv2.Metadata) (StreamT, error) {
	sID := formatStreamID(stream.GetName(), stream.GetGroup())
	sm, ok := s.schemaMap[sID]
	if !ok {
		return nil, errors.WithStack(ErrStreamNotExist)
	}
	return sm, nil
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "root-path", "/tmp", "the root path of database")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return ErrEmptyRootPath
	}
	return nil
}

func (s *service) Name() string {
	return "stream"
}

func (s *service) PreRun() error {
	schemas, err := s.metadata.Stream().List(context.TODO(), schema.ListOpt{})
	if err != nil {
		return err
	}

	s.schemaMap = make(map[string]*stream, len(schemas))
	s.l = logger.GetLogger(s.Name())
	for _, sa := range schemas {
		iRules, errIndexRules := s.metadata.IndexRules(context.TODO(), sa.Metadata)
		if errIndexRules != nil {
			return errIndexRules
		}
		sm, errTS := openStream(s.root, streamSpec{
			schema:     sa,
			indexRules: iRules,
		}, s.l)
		if errTS != nil {
			return errTS
		}
		id := formatStreamID(sm.name, sm.group)
		s.schemaMap[id] = sm
		s.l.Info().Str("id", id).Msg("initialize stream")
	}
	s.writeListener = setUpWriteCallback(s.l, s.schemaMap)
	return err
}

func (s *service) Serve() error {
	panic("implement me")
}

func (s *service) GracefulStop() {
	panic("implement me")
}

//NewService returns a new service
func NewService(_ context.Context, metadata metadata.Repo, pipeline queue.Queue) (Service, error) {
	return &service{
		metadata: metadata,
	}, nil
}
