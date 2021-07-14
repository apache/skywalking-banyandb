package event

import (
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

var (
	QueryEventKindVersion = common.KindVersion{
		Version: "v1",
		Kind:    "event-query",
	}
	// TopicQueryEvent is a bidirectional topic for request/response communication
	// between Liaison and Query module
	TopicQueryEvent = bus.BiTopic(QueryEventKindVersion.String())
)
