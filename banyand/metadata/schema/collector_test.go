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

package schema_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type stubGroupGetter struct {
	group *commonv1.Group
	err   error
}

func (s *stubGroupGetter) GetGroup(_ context.Context, _ string) (*commonv1.Group, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.group, nil
}

// failingBroadcaster fails the test if Broadcast is invoked. Used to assert
// that catalogs without a lifecycle (e.g. CATALOG_PROPERTY) short-circuit
// before reaching the broadcast path.
type failingBroadcaster struct {
	t *testing.T
}

func (f *failingBroadcaster) Broadcast(_ time.Duration, _ bus.Topic, _ bus.Message) ([]bus.Future, error) {
	f.t.Helper()
	f.t.Fatalf("Broadcast must not be called for catalogs without lifecycle inspection")
	return nil, errors.New("unreachable")
}

func TestCollectDataInfo_PropertyCatalogShortCircuits(t *testing.T) {
	registry := schema.NewInfoCollectorRegistry(logger.GetLogger("test"), &stubGroupGetter{
		group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "sw_property"},
			Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
		},
	})
	registry.SetDataBroadcaster(&failingBroadcaster{t: t})

	dataInfo, collectionErrs, err := registry.CollectDataInfo(context.Background(), "sw_property")
	require.NoError(t, err)
	assert.Empty(t, dataInfo)
	assert.Empty(t, collectionErrs)
}

func TestCollectLiaisonInfo_PropertyCatalogShortCircuits(t *testing.T) {
	registry := schema.NewInfoCollectorRegistry(logger.GetLogger("test"), &stubGroupGetter{
		group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "_deletion_task"},
			Catalog:  commonv1.Catalog_CATALOG_PROPERTY,
		},
	})
	registry.SetLiaisonBroadcaster(&failingBroadcaster{t: t})

	liaisonInfo, err := registry.CollectLiaisonInfo(context.Background(), "_deletion_task")
	require.NoError(t, err)
	assert.Empty(t, liaisonInfo)
}

func TestCollectDataInfo_UnspecifiedCatalogStillReturnsError(t *testing.T) {
	registry := schema.NewInfoCollectorRegistry(logger.GetLogger("test"), &stubGroupGetter{
		group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "weird_group"},
			Catalog:  commonv1.Catalog_CATALOG_UNSPECIFIED,
		},
	})
	registry.SetDataBroadcaster(&failingBroadcaster{t: t})

	dataInfo, collectionErrs, err := registry.CollectDataInfo(context.Background(), "weird_group")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported catalog type")
	assert.Nil(t, dataInfo)
	assert.Nil(t, collectionErrs)
}

func TestCollectLiaisonInfo_UnspecifiedCatalogStillReturnsError(t *testing.T) {
	registry := schema.NewInfoCollectorRegistry(logger.GetLogger("test"), &stubGroupGetter{
		group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: "weird_group"},
			Catalog:  commonv1.Catalog_CATALOG_UNSPECIFIED,
		},
	})
	registry.SetLiaisonBroadcaster(&failingBroadcaster{t: t})

	liaisonInfo, err := registry.CollectLiaisonInfo(context.Background(), "weird_group")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported catalog type")
	assert.Nil(t, liaisonInfo)
}
