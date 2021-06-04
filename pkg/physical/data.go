package physical

import (
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

type chunkIDs struct {
	chunkIDs []uint64
}

type entities struct {
	entities []*apiv1.Entity
}
