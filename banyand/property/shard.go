package property

import (
	"context"
	"fmt"
	"path"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	shardTemplate = "shard-%d"
	sourceField   = "_source"
	groupField    = "_group"
	nameField     = index.IndexModeName
	entityId      = "_entity_id"
)

var (
	sourceFieldKey = index.FieldKey{TagName: sourceField}
	entityFieldKey = index.FieldKey{TagName: entityId}
	groupFieldKey  = index.FieldKey{TagName: groupField}
	nameFieldKey   = index.FieldKey{TagName: nameField}
	projection     = []index.FieldKey{sourceFieldKey}
)

type shard struct {
	id       common.ShardID
	l        *logger.Logger
	location string
	store    index.SeriesStore
}

func (s *shard) close() error {
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (db *database) newShard(ctx context.Context, root string, id common.ShardID, flushTimeoutSeconds int64,
) (*shard, error) {
	location := path.Join(db.location, fmt.Sprintf(shardTemplate, int(id)))
	si := &shard{
		l: logger.Fetch(ctx, "shard"+strconv.Itoa(int(id))),
	}
	opts := inverted.StoreOpts{
		Path:         location,
		Logger:       si.l,
		Metrics:      inverted.NewMetrics(db.of, "shard"),
		BatchWaitSec: flushTimeoutSeconds,
	}
	var err error
	if si.store, err = inverted.NewStore(opts); err != nil {
		return nil, err
	}
	return si, nil
}

func (s *shard) update(id []byte, property *propertyv1.Property) error {
	pj, err := protojson.Marshal(property)
	if err != nil {
		return err
	}
	sourceField := index.NewBytesField(sourceFieldKey, pj)
	sourceField.NoSort = true
	sourceField.Store = true
	entityField := index.NewBytesField(entityFieldKey, []byte(property.Metadata.Id))
	entityField.Index = true
	groupField := index.NewBytesField(groupFieldKey, []byte(property.Metadata.Container.Group))
	groupField.Index = true
	nameField := index.NewBytesField(nameFieldKey, []byte(property.Metadata.Container.Name))
	nameField.Index = true

	doc := index.Document{
		EntityValues: id,
		Fields:       []index.Field{entityField, groupField, nameField, sourceField},
	}
	for _, t := range property.Tags {
		tv, err := pbv1.MarshalTagValue(t.Value)
		if err != nil {
			return err
		}
		tagField := index.NewBytesField(index.FieldKey{IndexRuleID: uint32(convert.HashStr(t.Key))}, tv)
		tagField.Index = true
		tagField.NoSort = true
		doc.Fields = append(doc.Fields, tagField)
	}
	return s.store.UpdateSeriesBatch(index.Batch{
		Documents: index.Documents{doc},
	})
}

func (s *shard) delete(docID [][]byte) error {
	return s.store.Delete(docID)
}

func (s *shard) search(ctx context.Context, indexQuery index.Query, limit int,
) (data [][]byte, err error) {
	tracer := query.GetTracer(ctx)
	if tracer != nil {
		span, _ := tracer.StartSpan(ctx, "property.search")
		span.Tagf("query", "%s", indexQuery.String())
		defer func() {
			if data != nil {
				span.Tagf("matched", "%d", len(data))
			}
			if err != nil {
				span.Error(err)
			}
			span.Stop()
		}()
	}
	ss, err := s.store.Search(ctx, projection, indexQuery, limit)
	if err != nil {
		return nil, err
	}
	if len(ss) == 0 {
		return nil, nil
	}
	data = make([][]byte, 0, len(ss))
	for _, s := range ss {
		data = append(data, s.Fields[sourceField])
	}
	return data, nil
}
