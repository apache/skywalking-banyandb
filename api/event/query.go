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
	TopicQueryEvent = bus.UniTopic(QueryEventKindVersion.String())
)
