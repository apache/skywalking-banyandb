package tsdb

import (
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/robfig/cron/v3"
)

type retentionController struct {
	segment   *segmentController
	scheduler cron.Schedule
	stopped   bool
	stopMux   sync.Mutex
	stopCh    chan struct{}
	duration  time.Duration
	l         *logger.Logger
}

func newRetentionController(segment *segmentController, ttl IntervalRule) (*retentionController, error) {
	var expr string
	switch ttl.Unit {
	case HOUR:
		// Every hour on the 5th minute
		expr = "5 *"
	case DAY:
		// Every day on 00:05
		expr = "5 0"

	}
	parser := cron.NewParser(cron.Minute | cron.Hour)
	scheduler, err := parser.Parse(expr)
	if err != nil {
		return nil, err
	}
	return &retentionController{
		segment:   segment,
		scheduler: scheduler,
		stopCh:    make(chan struct{}),
		l:         segment.l.Named("retention-controller"),
		duration:  ttl.EstimatedDuration(),
	}, nil
}

func (rc *retentionController) start() {
	rc.stopMux.Lock()
	if rc.stopped {
		return
	}
	rc.stopMux.Unlock()
	go rc.run()
}

func (rc *retentionController) run() {
	rc.l.Info().Msg("start")
	now := rc.segment.clock.Now()
	for {
		next := rc.scheduler.Next(now)
		timer := rc.segment.clock.Timer(next.Sub(now))
		for {
			select {
			case now = <-timer.C:
				rc.l.Info().Time("now", now).Msg("wake")
				if err := rc.segment.remove(now.Add(-rc.duration)); err != nil {
					rc.l.Error().Err(err)
				}
			case <-rc.stopCh:
				timer.Stop()
				rc.l.Info().Msg("stop")
				return
			}
			break
		}
	}

}

func (rc *retentionController) stop() {
	rc.stopMux.Lock()
	defer rc.stopMux.Unlock()
	if rc.stopped {
		return
	}
	rc.stopped = true
	close(rc.stopCh)
}
