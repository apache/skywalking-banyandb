package property

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

const (
	dirPerm        = 0o700
	filePermission = 0o600
	lockFilename   = "lock"
)

var (
	lfs           = fs.NewLocalFileSystemWithLogger(logger.GetLogger("property"))
	propertyScope = observability.RootScope.SubScope("property")
)

type database struct {
	lock          fs.File
	logger        *logger.Logger
	location      string
	flushInterval time.Duration
	of            *observability.Factory
	sLst          atomic.Pointer[[]*shard]
	closed        atomic.Bool
}

func openDB(ctx context.Context, location string, flushInterval time.Duration, of *observability.Factory) (*database, error) {
	loc := filepath.Clean(location)
	lfs.MkdirIfNotExist(loc, dirPerm)
	l := logger.GetLogger("property")

	db := &database{
		location:      loc,
		logger:        l,
		of:            of,
		flushInterval: flushInterval,
	}
	db.load(ctx)
	db.logger.Info().Str("path", loc).Msg("initialized")
	lockPath := filepath.Join(loc, lockFilename)
	lock, err := lfs.CreateLockFile(lockPath, filePermission)
	if err != nil {
		logger.Panicf("cannot create lock file %s: %s", lockPath, err)
	}
	db.lock = lock
	observability.MetricsCollector.Register(loc, db.collect)
	return db, nil
}

func (db *database) load(ctx context.Context) error {
	if db.closed.Load() {
		return errors.New("database is closed")
	}
	return walkDir(db.location, "shard-", func(suffix string) error {
		id, err := strconv.Atoi(suffix)
		if err != nil {
			return err
		}
		_, err = db.loadShard(ctx, common.ShardID(id))
		return err
	})
}

func (db *database) update(ctx context.Context, shardID common.ShardID, id []byte, property *propertyv1.Property) error {
	sd, err := db.loadShard(ctx, shardID)
	if err != nil {
		return err
	}
	err = sd.update(id, property)
	if err != nil {
		return err
	}
	return nil
}

func (db *database) delete(docIDs [][]byte) error {
	sLst := db.sLst.Load()
	var err error
	for _, s := range *sLst {
		multierr.AppendInto(&err, s.delete(docIDs))
	}
	return err
}

func (db *database) query(ctx context.Context, req *propertyv1.QueryRequest) ([][]byte, error) {
	iq, err := inverted.BuildPropertyQuery(req, groupField, entityId)
	if err != nil {
		return nil, err
	}
	sLst := db.sLst.Load()
	var res [][]byte
	for _, s := range *sLst {
		r, err := s.search(ctx, iq, int(req.Limit))
		if err != nil {
			return nil, err
		}
		res = append(res, r...)
	}
	return res, nil
}

func (db *database) loadShard(ctx context.Context, id common.ShardID) (*shard, error) {
	sLst := db.sLst.Load()
	for _, s := range *sLst {
		if s.id == id {
			return s, nil
		}
	}
	sd, err := db.newShard(context.WithValue(ctx, logger.ContextKey, db.logger), db.location, id, int64(db.flushInterval.Seconds()))
	if err != nil {
		return nil, err
	}
	*sLst = append(*sLst, sd)
	db.sLst.Store(sLst)
	return sd, nil
}

func (db *database) close() error {
	if db.closed.Swap(true) {
		return nil
	}
	sLst := db.sLst.Load()
	var err error
	if sLst != nil {
		for _, s := range *sLst {
			multierr.AppendInto(&err, s.close())
		}
	}
	db.lock.Close()
	return err
}

func (d *database) collect() {
	if d.closed.Load() {
		return
	}
	sLst := d.sLst.Load()
	if sLst == nil {
		return
	}
	for _, s := range *sLst {
		s.store.CollectMetrics(strconv.Itoa(int(s.id)))
	}
}

type walkFn func(suffix string) error

func walkDir(root, prefix string, wf walkFn) error {
	for _, f := range lfs.ReadDir(root) {
		if !f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		segs := strings.Split(f.Name(), "-")
		errWalk := wf(segs[len(segs)-1])
		if errWalk != nil {
			return errors.WithMessagef(errWalk, "failed to load: %s", f.Name())
		}
	}
	return nil
}
