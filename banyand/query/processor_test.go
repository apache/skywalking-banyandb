package query

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/series/trace"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestQueryProcessor(t *testing.T) {
	tester := require.New(t)
	// Bootstrap logger system
	err := logger.Bootstrap()
	tester.NoError(err)

	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	tester.NoError(err)
	tester.NotNil(repo)

	// Init `Database` module
	db, err := storage.NewDB(context.TODO(), repo)
	tester.NoError(err)
	tester.NoError(db.FlagSet().Parse(nil))

	// Init `Trace` module
	traceSvc, err := trace.NewService(context.TODO(), db, repo)
	tester.NoError(err)

	// Init `Query` module
	executor, err := NewExecutor(context.TODO(), repo, nil, traceSvc, traceSvc)
	tester.NotNil(executor)

	// :PreRun:
	// 1) TraceSeries,
	// 2) Database
	err = traceSvc.PreRun()
	tester.NoError(err)

	err = db.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	tests := []struct {
		// name of the test case
		name string
		// dataSetup allows to prepare data in advance for testing
		dataSetup func() error
		// queryGenerator is used to generate a Query
		queryGenerator func() *v1.EntityCriteria
		// wantLen is the length of entities expected to return
		wantLen int
	}{
		{
			name: "Query Trace ID when no initial data is given",
			queryGenerator: func() *v1.EntityCriteria {
				builder := fb.NewCriteriaBuilder()
				return builder.BuildEntityCriteria(
					fb.AddLimit(5),
					fb.AddOffset(10),
					builder.BuildMetaData("default", "sw"),
					builder.BuildFields("trace_id", "=", "123"),
				)
			},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester := require.New(t)
			now := time.Now().UnixNano()
			m := bus.NewMessage(bus.MessageID(now), tt.queryGenerator())
			f, err := repo.Publish(event.TopicQueryEvent, m)
			tester.NoError(err)
			tester.NotNil(f)
			msg, err := f.Get()
			tester.NoError(err)
			tester.NotNil(msg)
			tester.Len(msg.Data(), tt.wantLen)
		})
	}
}
