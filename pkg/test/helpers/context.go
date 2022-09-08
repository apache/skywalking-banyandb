package helpers

import (
	"time"

	grpclib "google.golang.org/grpc"
)

type SharedContext struct {
	Connection *grpclib.ClientConn
	BaseTime   time.Time
}
