package property

import (
	"context"
	"errors"
	"path"
	"runtime/debug"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

const defaultFlushTimeout = 5 * time.Second

var (
	errEmptyRootPath         = errors.New("root path is empty")
	_                Service = (*service)(nil)
)

type service struct {
	metadata     metadata.Repo
	pipeline     queue.Server
	omr          observability.MetricsRegistry
	l            *logger.Logger
	root         string
	nodeID       string
	flushTimeout time.Duration
	db           *database
	close        chan struct{}
}

func (s *service) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	var protoReq proto.Message
	defer func() {
		if err := recover(); err != nil {
			s.l.Error().Interface("err", err).RawJSON("req", logger.Proto(protoReq)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic: %v", err))
		}
	}()
	switch d := message.Data().(type) {
	case *propertyv1.InternalUpdateRequest:
		protoReq = d
		if d.Property == nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("property is nil"))
			return
		}
		if d.Property.Tags == nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("tags is nil"))
			return
		}
		if len(d.Id) == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("id is empty"))
			return
		}
		err := s.db.update(ctx, common.ShardID(d.ShardId), d.Id, d.Property)
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to update property: %v", err))
			return
		}
		resp = bus.NewMessage(bus.MessageID(now), &propertyv1.ApplyResponse{
			Created: true,
			TagsNum: uint32(len(d.Property.Tags)),
		})
	case *propertyv1.InternalDeleteRequest:
		protoReq = d
		if len(d.Ids) == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("id is empty"))
			return
		}
		err := s.db.delete(d.Ids)
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to delete property: %v", err))
			return
		}
		resp = bus.NewMessage(bus.MessageID(now), &propertyv1.DeleteResponse{
			Deleted: true,
		})
	case *propertyv1.QueryRequest:
		protoReq = d
		if len(d.Groups) == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("groups is empty"))
			return
		}
		if d.Container == "" {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("container is empty"))
			return
		}
		if len(d.TagProjection) == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("tag projection is empty"))
			return
		}
		if d.Limit == 0 {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("limit is 0"))
			return
		}
		var tracer *query.Tracer
		var span *query.Span
		if d.Trace {
			tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
			span, ctx = tracer.StartSpan(ctx, "data-%s", s.nodeID)
			span.Tag("req", string(logger.Proto(protoReq)))
			defer func() {
				span.Stop()
			}()
		}
		sources, err := s.db.query(ctx, d)
		if err != nil {
			if tracer != nil {
				span.Error(err)
				resp = bus.NewMessage(bus.MessageID(now), &propertyv1.InternalQueryResponse{
					Trace: tracer.ToProto(),
				})
				return
			}
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to query property: %v", err))
			return
		}
		qResp := &propertyv1.InternalQueryResponse{
			Sources: sources,
		}
		if tracer != nil {
			qResp.Trace = tracer.ToProto()
		}
		resp = bus.NewMessage(bus.MessageID(now), qResp)
		return

	default:
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
	}
	return
}

func (s *service) FlagSet() *run.FlagSet {
	flagS := run.NewFlagSet("storage")
	flagS.StringVar(&s.root, "property-root-path", "/tmp", "the root path of database")
	flagS.DurationVar(&s.flushTimeout, "property-flush-timeout", defaultFlushTimeout, "the memory data timeout of measure")
	return flagS
}

func (s *service) Validate() error {
	if s.root == "" {
		return errEmptyRootPath
	}
	return nil
}

func (s *service) Name() string {
	return "measure"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_DATA
}

func (s *service) PreRun(ctx context.Context) error {
	s.l = logger.GetLogger(s.Name())
	path := path.Join(s.root, s.Name())
	observability.UpdatePath(path)
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	s.nodeID = node.NodeID

	var err error
	s.db, err = openDB(ctx, path, s.flushTimeout, s.omr.With(propertyScope))
	if err != nil {
		return err
	}
	return multierr.Combine(
		s.pipeline.Subscribe(data.TopicPropertyUpdate, s),
		s.pipeline.Subscribe(data.TopicPropertyDelete, s),
		s.pipeline.Subscribe(data.TopicPropertyQuery, s),
	)
}

func (s *service) Serve() run.StopNotify {
	return s.close
}

func (s *service) GracefulStop() {
	close(s.close)
	err := s.db.close()
	if err != nil {
		s.l.Err(err).Msg("Fail to close the property module")
	}
}

// NewService returns a new service.
func NewService(metadata metadata.Repo, pipeline queue.Server, omr observability.MetricsRegistry) (Service, error) {
	return &service{
		metadata: metadata,
		pipeline: pipeline,
		omr:      omr,
		db:       &database{},
		close:    make(chan struct{}),
	}, nil
}
