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

package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// keepInterval is how often a producer checks that its connection to a data
// node's cluster server is up, as lifecycle's native metrics client does.
const keepInterval = 10 * time.Second

// destinationNodeName is the name a producer registers its data node under on
// its own client. The client holds no other node.
const destinationNodeName = "native-log-destination"

// Destination is where a producer writes its logs: one data node's cluster
// server, or one liaison's public write API.
type Destination struct {
	// Addr is the gRPC address. Native logging needs it: NewProducer
	// refuses an empty one rather than drop every event.
	Addr string
	// CACert verifies the server when TLS is on; empty, the system roots do.
	CACert string
	// Username and Password are sent to a liaison whose RBAC is on.
	Username string
	Password string
	// Public selects a liaison's public StreamService/Write. Otherwise Addr
	// is a data node's cluster server, written to as TopicStreamWrite.
	Public bool
	TLS    bool
	// InsecureSkipVerify skips verifying the server's certificate.
	InsecureSkipVerify bool
}

// RegisterDestinationFlags registers the flags of a producer that has no
// connection to BanyanDB to reuse, such as an FODC agent or proxy. Backup and
// lifecycle write to the data node they already serve, with its settings.
func RegisterDestinationFlags(fs *pflag.FlagSet, d *Destination) {
	fs.StringVar(&d.Addr, "logging-native-addr", "",
		"where to store this process's own logs: a data node's cluster server, or a liaison's gRPC port; "+
			"needed by --logging-native-enabled")
	fs.BoolVar(&d.TLS, "logging-native-tls", false, "connect to --logging-native-addr over TLS")
	fs.StringVar(&d.CACert, "logging-native-ca-cert", "",
		"the CA certificate that verifies --logging-native-addr; empty, the system roots do")
	fs.StringVar(&d.Username, "logging-native-username", "",
		"the user the logs are written as, when the liaison's authentication is on")
	fs.StringVar(&d.Password, "logging-native-password", "",
		"the password of --logging-native-username")
}

// ProducerConfig configures the service of a producer: a process with no
// storage engine and no metadata client, such as backup, lifecycle, or an FODC
// agent or proxy.
type ProducerConfig struct {
	// Native is the process's native logging configuration, as parsed from
	// its --logging-native-* flags.
	Native *logger.NativeLogging
	// Gauges publishes the counters; nil, a summary of what was dropped is
	// written to stderr at exit instead.
	Gauges      Gauges
	Node        NodeInfo
	Destination Destination
}

// NewProducer returns the service that stores a producer's own logs. It
// returns nil when native logging is off. Call it after logger.Init: it
// attaches the sink, so the lines logged from here on are kept until delivery
// starts in Serve.
//
// The node it writes to owns the schema (--logging-native-receiver-enabled):
// a producer cannot read it, so it treats it as established.
func NewProducer(cfg ProducerConfig) (*Service, error) {
	if cfg.Native == nil || !cfg.Native.Enabled {
		return nil, nil
	}
	if cfg.Destination.Addr == "" {
		return nil, errors.New("native logging needs the address of a node to write to")
	}
	sink := NewSink()
	s := &Service{
		sink:   sink,
		cfg:    cfg.Native,
		gauges: cfg.Gauges,
		node:   cfg.Node,
		closer: run.NewCloser(1),
		timing: defaultTiming,
	}
	dest, err := newDestination(cfg.Destination, s.timing.writeTimeout)
	if err != nil {
		return nil, err
	}
	s.dest = dest
	s.schema.Store(schemaReady)
	sink.Attach()
	return s, nil
}

// StartProducer runs s for a process with no run.Group, and returns the
// function that stops it. Stop drains within drainTimeout and is idempotent.
func StartProducer(s *Service) (stop func()) {
	if s == nil {
		return func() {}
	}
	_ = s.PreRun(context.Background())
	s.Serve()
	var once sync.Once
	return func() { once.Do(s.GracefulStop) }
}

// ProducerNodeID is the node_id of a producer: name when it is set, else
// POD_NAME, else the host name, else fallback.
func ProducerNodeID(name, fallback string) string {
	if name != "" {
		return name
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		return pod
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return fallback
}

// destination is a producer's connection and the delivery that uses it.
type destination struct {
	delivery queue.Delivery
	start    func()
	stop     func()
	public   bool
}

// nativeClientLogger is the logger of a producer's client. Everything the
// client logs is about native delivery, so it logs under NATIVE-LOG: a failed
// dial reaches the console and never native storage.
func nativeClientLogger() *logger.Logger {
	return logger.GetLogger(logger.NativeWriteModule).Named("client")
}

func newDestination(cfg Destination, timeout time.Duration) (*destination, error) {
	if cfg.Public {
		return newPublicDestination(cfg, timeout)
	}
	return newClusterDestination(cfg, timeout), nil
}

// newClusterDestination writes to a data node's cluster server, as
// lifecycle's migration does: TopicStreamWrite through a pub client, the
// delivery of a liaison addressed to one fixed node.
func newClusterDestination(cfg Destination, timeout time.Duration) *destination {
	l := nativeClientLogger()
	opts := []pub.Option{pub.WithLogger(l)}
	if cfg.TLS {
		opts = append(opts, pub.WithClientTLS(cfg.CACert, cfg.InsecureSkipVerify))
	}
	client := pub.NewWithoutMetadata(nil, opts...)
	var stopKeeper func()
	return &destination{
		delivery: queue.NewNodeDelivery(client, data.TopicStreamWrite, fixedNode(client), timeout),
		start: func() {
			stopKeeper = pub.KeepNode(client, pub.DataNode(destinationNodeName, cfg.Addr), keepInterval, l)
		},
		stop: func() {
			if stopKeeper != nil {
				stopKeeper()
			}
			client.GracefulStop()
		},
	}
}

// fixedNode locates the one node a producer's client holds. While the client
// has no healthy connection to it nothing can be sent, so the batch is
// retried; the keeper dials it again meanwhile.
func fixedNode(client queue.Client) func() (string, error) {
	return func() (string, error) {
		if len(client.HealthyNodes()) == 0 {
			return "", errDestinationUnready
		}
		return destinationNodeName, nil
	}
}

// newPublicDestination writes through a liaison's public StreamService/Write,
// as any client does: the liaison authorizes the write, builds the internal
// request and writes it to its write queue.
func newPublicDestination(cfg Destination, timeout time.Duration) (*destination, error) {
	opts, err := grpchelper.SecureOptions(nil, cfg.TLS, cfg.InsecureSkipVerify, cfg.CACert)
	if err != nil {
		return nil, fmt.Errorf("native log destination %s: %w", cfg.Addr, err)
	}
	// NewClient does not dial: it connects on the first write and reconnects
	// by itself, so a liaison that is not up yet costs a retried batch.
	conn, err := grpc.NewClient(cfg.Addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("native log destination %s: %w", cfg.Addr, err)
	}
	return &destination{
		delivery: &publicDelivery{
			client:   streamv1.NewStreamServiceClient(conn),
			username: cfg.Username,
			password: cfg.Password,
			timeout:  timeout,
		},
		start:  func() {},
		stop:   func() { _ = conn.Close() },
		public: true,
	}, nil
}

// publicDelivery writes each batch as one StreamService/Write stream, one
// WriteRequest per event. The liaison answers each event, so an event it
// refuses is counted on its own and the rest of the batch as written.
type publicDelivery struct {
	client   streamv1.StreamServiceClient
	username string
	password string
	timeout  time.Duration
}

func (p *publicDelivery) Deliver(ctx context.Context, batch []any) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if p.username != "" {
		ctx = grpcmetadata.AppendToOutgoingContext(ctx, "username", p.username, "password", p.password)
	}
	stream, err := p.client.Write(ctx)
	if err != nil {
		return openFailure(err)
	}
	sent := make(map[uint64]*request, len(batch))
	for i, item := range batch {
		r, ok := item.(*request)
		if !ok {
			continue
		}
		// seq is unique in the process and never 0, which the API rejects.
		r.MessageId = r.seq
		sent[r.MessageId] = r
		if sendErr := stream.Send(r.WriteRequest); sendErr != nil {
			if i == 0 {
				return openFailure(sendErr)
			}
			return fmt.Errorf("the native log write stream failed: %w", sendErr)
		}
	}
	if err = stream.CloseSend(); err != nil {
		return fmt.Errorf("the native log write stream failed: %w", err)
	}
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return fmt.Errorf("the native log write stream failed: %w", recvErr)
		}
		if resp.GetStatusCode() == modelv1.Status_STATUS_SUCCEED {
			continue
		}
		if r, ok := sent[resp.GetMessageId()]; ok {
			reason := refusalReason(resp.GetStatusCode())
			r.refused.Store(&reason)
		}
	}
}

// openFailure classifies a failure before any event was taken. A liaison that
// cannot be reached yet is retried, because nothing was written; anything
// else, such as refused credentials, ends the batch.
func openFailure(err error) error {
	if status.Code(err) == codes.Unavailable {
		return queue.Retryable(fmt.Errorf("%w: %w", errDestinationUnready, err))
	}
	return fmt.Errorf("the native log write stream was refused: %w", err)
}

// refusalReason names the drop reason of an event the liaison refused.
func refusalReason(st modelv1.Status) string {
	switch st {
	case modelv1.Status_STATUS_NOT_FOUND, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, modelv1.Status_STATUS_EXPIRED_SCHEMA:
		return reasonSchemaMissing
	default:
		return reasonPublishFailed
	}
}
