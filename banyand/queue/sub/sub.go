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

// Package sub implements the queue server.
package sub

import (
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func checkVersionCompatibility(versionInfo *clusterv1.VersionInfo) (*clusterv1.VersionCompatibility, modelv1.Status) {
	if versionInfo == nil {
		return &clusterv1.VersionCompatibility{
			Supported:                   true,
			ServerApiVersion:            apiversion.Version,
			SupportedApiVersions:        []string{apiversion.Version},
			ServerFileFormatVersion:     storage.GetCurrentVersion(),
			SupportedFileFormatVersions: storage.GetCompatibleVersions(),
			Reason:                      "No version info provided, assuming compatible",
		}, modelv1.Status_STATUS_SUCCEED
	}

	serverAPIVersion := apiversion.Version
	serverFileFormatVersion := storage.GetCurrentVersion()
	compatibleFileFormatVersions := storage.GetCompatibleVersions()

	apiCompatible := versionInfo.ApiVersion == serverAPIVersion

	fileFormatCompatible := versionInfo.FileFormatVersion == serverFileFormatVersion
	if !fileFormatCompatible {
		for _, compatVer := range compatibleFileFormatVersions {
			if compatVer == versionInfo.FileFormatVersion {
				fileFormatCompatible = true
				break
			}
		}
	}

	vc := &clusterv1.VersionCompatibility{
		ServerApiVersion:            serverAPIVersion,
		SupportedApiVersions:        []string{serverAPIVersion},
		ServerFileFormatVersion:     serverFileFormatVersion,
		SupportedFileFormatVersions: compatibleFileFormatVersions,
	}

	switch {
	case !apiCompatible && !fileFormatCompatible:
		vc.Supported = false
		vc.Reason = fmt.Sprintf("API version %s not supported (server: %s) and file format version %s not compatible (server: %s, supported: %v)",
			versionInfo.ApiVersion, serverAPIVersion, versionInfo.FileFormatVersion, serverFileFormatVersion, compatibleFileFormatVersions)
		return vc, modelv1.Status_STATUS_VERSION_UNSUPPORTED
	case !apiCompatible:
		vc.Supported = false
		vc.Reason = fmt.Sprintf("API version %s not supported (server: %s)", versionInfo.ApiVersion, serverAPIVersion)
		return vc, modelv1.Status_STATUS_VERSION_UNSUPPORTED
	case !fileFormatCompatible:
		vc.Supported = false
		vc.Reason = fmt.Sprintf("File format version %s not compatible (server: %s, supported: %v)",
			versionInfo.FileFormatVersion, serverFileFormatVersion, compatibleFileFormatVersions)
		return vc, modelv1.Status_STATUS_VERSION_UNSUPPORTED
	}

	vc.Supported = true
	vc.Reason = "Client version compatible with server"
	return vc, modelv1.Status_STATUS_SUCCEED
}

// streamIdentity captures the per-stream remote sender identity, pinned on the first message.
type streamIdentity struct {
	senderNode string
	senderRole string
	senderTier string
	group      string
	operation  string
	pinned     bool
}

func (s *server) Send(stream clusterv1.Service_SendServer) error {
	ctx := stream.Context()
	var topic *bus.Topic
	var dataCollection []any
	start := time.Now()
	identity := &streamIdentity{}

	defer func() {
		if topic == nil || !identity.pinned || s.metrics == nil {
			return
		}
		s.metrics.totalFinished.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
		s.metrics.totalLatency.Observe(time.Since(start).Seconds(), identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		writeEntity, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.handleEOF(stream, topic, dataCollection, writeEntity, identity, start)
			return nil
		}
		if err != nil {
			return s.handleRecvError(err)
		}

		if versionErr := s.checkVersionAndReply(stream, writeEntity); versionErr != nil {
			return versionErr
		}

		topic, err = s.resolveTopic(stream, writeEntity, topic, identity)
		if err != nil {
			continue
		}
		if topic == nil {
			continue
		}

		s.pinIdentity(identity, writeEntity, *topic)

		m, parseErr := s.parseMessage(stream, writeEntity, *topic, identity)
		if parseErr != nil {
			continue
		}

		if writeEntity.BatchMod {
			s.handleBatch(&dataCollection, writeEntity, &start)
			continue
		}

		s.dispatchMessage(stream, writeEntity, *topic, m, identity, &start)
	}
}

// checkVersionAndReply returns an error if the version is incompatible and sends the rejection response.
func (s *server) checkVersionAndReply(stream clusterv1.Service_SendServer, writeEntity *clusterv1.SendRequest) error {
	if writeEntity.VersionInfo == nil {
		return nil
	}
	vc, versionStatus := checkVersionCompatibility(writeEntity.VersionInfo)
	if versionStatus == modelv1.Status_STATUS_SUCCEED {
		return nil
	}
	s.log.Warn().
		Str("client_api_version", writeEntity.VersionInfo.ApiVersion).
		Str("client_file_format_version", writeEntity.VersionInfo.FileFormatVersion).
		Str("reason", vc.Reason).
		Msg("version compatibility check failed")
	if errSend := stream.Send(&clusterv1.SendResponse{
		MessageId:            writeEntity.MessageId,
		Status:               versionStatus,
		Error:                vc.Reason,
		VersionCompatibility: vc,
	}); errSend != nil {
		s.log.Error().Err(errSend).Msg("failed to send version incompatibility response")
	}
	return fmt.Errorf("version incompatibility: %s", vc.Reason)
}

// resolveTopic parses and validates the topic from the write entity.
// Returns a non-nil error (sentinel) to signal the caller to `continue`.
func (s *server) resolveTopic(
	stream clusterv1.Service_SendServer,
	writeEntity *clusterv1.SendRequest,
	current *bus.Topic,
	identity *streamIdentity,
) (*bus.Topic, error) {
	if writeEntity.Topic == "" || current != nil {
		if current == nil {
			s.replyWithErrType(stream, writeEntity, nil, "topic is empty", identity, "empty_topic")
			return nil, fmt.Errorf("empty")
		}
		return current, nil
	}
	t, ok := data.TopicMap[writeEntity.Topic]
	if !ok {
		s.replyWithErrType(stream, writeEntity, nil, "invalid topic", identity, "invalid_topic")
		return nil, fmt.Errorf("invalid")
	}
	return &t, nil
}

// pinIdentity pins the sender identity on the first message.
func (s *server) pinIdentity(identity *streamIdentity, writeEntity *clusterv1.SendRequest, topic bus.Topic) {
	if !identity.pinned {
		identity.senderNode = writeEntity.GetSenderNode()
		identity.senderRole = writeEntity.GetSenderRole()
		identity.senderTier = writeEntity.GetSenderTier()
		identity.operation = data.OperationOf(topic)
		identity.group = writeEntity.GetGroup()
		identity.pinned = true
		return
	}
	if g := writeEntity.GetGroup(); g != "" {
		identity.group = g
	}
}

// parseMessage deserializes the wire body into a bus.Message. Returns a sentinel error to `continue` on failure.
func (s *server) parseMessage(
	stream clusterv1.Service_SendServer,
	writeEntity *clusterv1.SendRequest,
	topic bus.Topic,
	identity *streamIdentity,
) (bus.Message, error) {
	reqSupplier, ok := data.TopicRequestMap[topic]
	if !ok {
		s.replyWithErrType(stream, writeEntity, nil, "unknown topic", identity, "unknown_topic")
		return bus.Message{}, fmt.Errorf("unknown")
	}
	req := reqSupplier()
	if req == nil {
		return bus.NewMessage(bus.MessageID(writeEntity.MessageId), writeEntity.Body), nil
	}
	if errUnmarshal := proto.Unmarshal(writeEntity.Body, req); errUnmarshal != nil {
		s.replyWithErrType(stream, writeEntity, errUnmarshal, "failed to unmarshal message", identity, "unmarshal_error")
		return bus.Message{}, errUnmarshal
	}
	return bus.NewMessage(bus.MessageID(writeEntity.MessageId), req), nil
}

// dispatchMessage invokes the listener and sends back the response.
func (s *server) dispatchMessage(
	stream clusterv1.Service_SendServer,
	writeEntity *clusterv1.SendRequest,
	topic bus.Topic,
	m bus.Message,
	identity *streamIdentity,
	start *time.Time,
) {
	listeners := s.getListeners(topic)
	if len(listeners) == 0 {
		s.replyWithErrType(stream, writeEntity, nil, "no listener found", identity, "no_listener")
		return
	}
	if len(listeners) > 1 {
		logger.Panicf("multiple listeners found for topic %s", topic)
	}
	// Tick started immediately before Rev so that started and finished are always paired;
	// pre-Rev early returns (no-listener) produce no metric change.
	if s.metrics != nil {
		s.metrics.totalStarted.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
		s.metrics.totalMessageStarted.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
	}
	m = listeners[0].Rev(stream.Context(), m)
	// The BatchMod fork in Send routes each SendRequest to exactly one of
	// handleBatch→handleEOF (batch) or dispatchMessage (non-batch), so no message is double-counted.
	if s.metrics != nil {
		s.metrics.totalMessageFinished.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier)
	}
	if m.Data() == nil {
		if errSend := stream.Send(&clusterv1.SendResponse{MessageId: writeEntity.MessageId}); errSend != nil {
			s.log.Error().Stringer("request", writeEntity).Err(errSend).Msg("failed to send empty response")
			if s.metrics != nil {
				s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, "send_error")
			}
		}
		return
	}
	responseBody, marshalErr := s.marshalResponse(stream, writeEntity, topic, m, identity)
	if marshalErr != nil {
		return
	}
	if sendErr := stream.Send(&clusterv1.SendResponse{MessageId: writeEntity.MessageId, Body: responseBody}); sendErr != nil {
		s.log.Error().Stringer("request", writeEntity).Dur("latency", time.Since(*start)).Err(sendErr).Msg("failed to send query response")
		if s.metrics != nil {
			s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, "send_error")
		}
	}
}

// marshalResponse converts a bus.Message data to a wire body. Returns sentinel error to stop dispatch on failure.
func (s *server) marshalResponse(
	stream clusterv1.Service_SendServer,
	writeEntity *clusterv1.SendRequest,
	topic bus.Topic,
	m bus.Message,
	identity *streamIdentity,
) ([]byte, error) {
	switch d := m.Data().(type) {
	case proto.Message:
		body, marshalErr := proto.Marshal(d)
		if marshalErr != nil {
			s.replyWithErrType(stream, writeEntity, marshalErr, "failed to marshal message", identity, "marshal_error")
			return nil, marshalErr
		}
		return body, nil
	case []byte:
		rawAllowed := (topic == data.TopicInternalMeasureQuery && data.MeasureWireModeRaw()) ||
			(topic == data.TopicTraceQuery && data.TraceWireModeRaw())
		if !rawAllowed {
			s.replyWithErrType(stream, writeEntity, nil, fmt.Sprintf("invalid response: unexpected raw body on topic %s", topic), identity, "marshal_error")
			return nil, fmt.Errorf("invalid raw body")
		}
		return d, nil
	case *common.Error:
		s.replyWithErrType(stream, writeEntity, nil, d.Error(), identity, "handler_error")
		return nil, fmt.Errorf("handler error")
	default:
		s.replyWithErrType(stream, writeEntity, nil, fmt.Sprintf("invalid response: %T", d), identity, "handler_error")
		return nil, fmt.Errorf("invalid response type")
	}
}

func (s *server) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	listeners, ok := s.listeners[topic]
	if ok {
		listeners = append(listeners, listener)
		s.listeners[topic] = listeners
		return nil
	}
	listeners = make([]bus.MessageListener, 0)
	listeners = append(listeners, listener)
	s.listeners[topic] = listeners
	s.topicMap[topic.String()] = topic
	return nil
}

func (s *server) getListeners(topic bus.Topic) []bus.MessageListener {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	return s.listeners[topic]
}

func (s *server) replyWithErrType(
	stream clusterv1.Service_SendServer,
	writeEntity *clusterv1.SendRequest,
	err error,
	message string,
	identity *streamIdentity,
	errType string,
) {
	s.log.Error().Stringer("request", writeEntity).Err(err).Msg(message)
	if s.metrics != nil {
		s.metrics.totalErr.Inc(1, identity.operation, identity.group, identity.senderNode, identity.senderRole, identity.senderTier, errType)
	}
	var msgID uint64
	if writeEntity != nil {
		msgID = writeEntity.MessageId
	}
	if errResp := stream.Send(&clusterv1.SendResponse{
		MessageId: msgID,
		Error:     message,
	}); errResp != nil {
		s.log.Error().Err(errResp).AnErr("original", err).Stringer("request", writeEntity).Msg("failed to send error response")
	}
}
