package property

import "github.com/apache/skywalking-banyandb/pkg/run"

type Service interface {
	run.PreRunner
	run.Config
	run.Service
}
