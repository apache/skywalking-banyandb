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

package property

import (
	"context"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

// DirectInsert implements DirectService.DirectInsert.
func (s *service) DirectInsert(ctx context.Context, _ string, shardID uint32, id []byte, prop *propertyv1.Property) error {
	return s.db.update(ctx, common.ShardID(shardID), id, prop)
}

// DirectUpdate implements DirectService.DirectUpdate.
func (s *service) DirectUpdate(ctx context.Context, group string, shardID uint32, id []byte, prop *propertyv1.Property) error {
	// query the older properties for delete
	olderProperties, err := s.db.query(ctx, &propertyv1.QueryRequest{
		Groups: []string{group},
		Name:   prop.Metadata.Name,
		Ids:    []string{prop.Id},
	})
	if err != nil {
		return err
	}
	defer func() {
		olderIDs := make([][]byte, 0, len(olderProperties))
		for _, p := range olderProperties {
			olderIDs = append(olderIDs, p.id)
		}
		if len(olderIDs) > 0 {
			err = s.db.delete(ctx, olderIDs)
			if err != nil {
				s.l.Warn().Err(err).Msg("failed to delete old properties in direct update")
			}
		}
	}()
	return s.db.update(ctx, common.ShardID(shardID), id, prop)
}

// DirectDelete implements DirectService.DirectDelete.
func (s *service) DirectDelete(ctx context.Context, ids [][]byte) error {
	return s.db.delete(ctx, ids)
}

// DirectQuery implements DirectService.DirectQuery.
func (s *service) DirectQuery(ctx context.Context, req *propertyv1.QueryRequest) ([]*WithDeleteTime, error) {
	results, queryErr := s.db.query(ctx, req)
	if queryErr != nil {
		return nil, queryErr
	}
	props := make([]*WithDeleteTime, 0, len(results))
	for _, r := range results {
		prop := &propertyv1.Property{}
		if unmarshalErr := protojson.Unmarshal(r.source, prop); unmarshalErr != nil {
			s.l.Warn().Err(unmarshalErr).Msg("failed to unmarshal property")
			continue
		}
		props = append(props, &WithDeleteTime{
			Property:   prop,
			DeleteTime: r.deleteTime,
		})
	}
	return props, nil
}

// DirectGet implements DirectService.DirectGet.
func (s *service) DirectGet(ctx context.Context, group, name, id string) (*propertyv1.Property, error) {
	req := &propertyv1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		Ids:    []string{id},
	}
	results, queryErr := s.DirectQuery(ctx, req)
	if queryErr != nil {
		return nil, queryErr
	}
	for _, r := range results {
		if r.DeleteTime == 0 {
			return r.Property, nil
		}
	}
	return nil, nil
}

// DirectExist implements DirectService.DirectExist.
func (s *service) DirectExist(ctx context.Context, group, name, id string) (bool, error) {
	req := &propertyv1.QueryRequest{
		Groups: []string{group},
		Name:   name,
		Ids:    []string{id},
	}
	results, queryErr := s.db.query(ctx, req)
	if queryErr != nil {
		return false, queryErr
	}
	for _, r := range results {
		if r.deleteTime == 0 {
			return true, nil
		}
	}
	return false, nil
}

// DirectRepair implements DirectService.DirectRepair.
func (s *service) DirectRepair(ctx context.Context, shardID uint64, id []byte, prop *propertyv1.Property, deleteTime int64) error {
	return s.db.repair(ctx, id, shardID, prop, deleteTime)
}
