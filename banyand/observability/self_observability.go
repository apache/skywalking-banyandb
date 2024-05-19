package observability

import (
	"context"

	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
)

const internalGroupJSON = `{
    "metadata": {
        "name": "self_observability"
    },
    "catalog": "CATALOG_MEASURE",
    "resourceOpts": {
        "shardNum": 1,
        "segmentInterval": {
            "unit": "UNIT_DAY",
            "num": 1
        },
        "ttl": {
            "unit": "UNIT_DAY",
            "num": 3
        }
    }
}`

func createInternalGroup(e metadata.Repo) error {
	g := &commonv1.Group{}
	if err := protojson.Unmarshal([]byte(internalGroupJSON), g); err != nil {
		return err
	}
	if err := e.GroupRegistry().CreateGroup(context.TODO(), g); err != nil {
		return err
	}
	return nil
}
