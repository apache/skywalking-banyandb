package query

import (
	"context"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const (
	moduleName = "query-processor"
)

var (
	_ Executor                  = (*queryProcessor)(nil)
	_ bus.MessageListener       = (*queryProcessor)(nil)
	_ executor.ExecutionContext = (*queryProcessor)(nil)
)

type queryProcessor struct {
	index.Repo
	series.UniModel
	schemaRepo series.SchemaRepo
	log        *logger.Logger
	liaison    bus.Subscriber
}

func (q *queryProcessor) Rev(message bus.Message) (resp bus.Message) {
	data, ok := message.Data().([]byte)
	if !ok {
		q.log.Warn().Msg("invalid event data type")
		return
	}
	queryCriteria := v1.GetRootAsEntityCriteria(data, 0)
	q.log.Info().
		Msg("received a query event")
	analyzer := logical.NewAnalyzer(q.schemaRepo)
	metadata := &common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        queryCriteria.Metadata(nil),
	}
	s, err := analyzer.BuildTraceSchema(context.TODO(), *metadata)
	if err != nil {
		return
	}

	p, err := analyzer.Analyze(context.TODO(), queryCriteria, metadata, s)
	if err != nil {
		return
	}

	entities, err := p.Execute(q)
	if err != nil {
		return
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), entities)

	return
}

func (q *queryProcessor) Name() string {
	return moduleName
}

func (q *queryProcessor) PreRun() error {
	q.log = logger.GetLogger(moduleName)
	return q.liaison.Subscribe(event.TopicShardEvent, q)
}
