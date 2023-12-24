package stream

import (
	"context"
	"path"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IndexDB is the interface of index database.
type IndexDB interface {
	Write(docs index.Documents) error
	Search(ctx context.Context, seriesList *pbv1.SeriesList, filter index.Filter) (posting.List, error)
}

type elementIndex struct {
	store index.SeriesStore
	l     *logger.Logger
}

func newElementIndex(ctx context.Context, root string) (*elementIndex, error) {
	si := &elementIndex{
		l: logger.Fetch(ctx, "element_index"),
	}
	var err error
	if si.store, err = inverted.NewStore(inverted.StoreOpts{
		Path:   path.Join(root, "idx"),
		Logger: si.l,
	}); err != nil {
		return nil, err
	}
	return si, nil
}

func (s *elementIndex) Write(docs index.Documents) error {
	return s.store.Batch(docs)
}

var rangeOpts = index.RangeOpts{}

func convertIndexSeriesToSeriesList(indexSeries []index.Series) (pbv1.SeriesList, error) {
	seriesList := make(pbv1.SeriesList, 0, len(indexSeries))
	for _, s := range indexSeries {
		var series pbv1.Series
		series.ID = s.ID
		if err := series.Unmarshal(s.EntityValues); err != nil {
			return nil, err
		}
		seriesList = append(seriesList, &series)
	}
	return seriesList, nil
}

func (s *elementIndex) Search(ctx context.Context, seriesList *pbv1.SeriesList, filter index.Filter) (posting.List, error) {
	plFilter, err := filter.Execute(func(ruleType databasev1.IndexRule_Type) (index.Searcher, error) {
		return s.store, nil
	}, 0)
	if err != nil {
		return nil, err
	}
	return plFilter, nil
}

func filterSeriesList(seriesList pbv1.SeriesList, filter posting.List) pbv1.SeriesList {
	for i := 0; i < len(seriesList); i++ {
		if !filter.Contains(uint64(seriesList[i].ID)) {
			seriesList = append(seriesList[:i], seriesList[i+1:]...)
			i--
		}
	}
	return seriesList
}

func appendSeriesList(dest, src pbv1.SeriesList, filter posting.List) pbv1.SeriesList {
	for i := 0; i < len(src); i++ {
		if !filter.Contains(uint64(src[i].ID)) {
			continue
		}
		dest = append(dest, src[i])
	}
	return dest
}

func (s *elementIndex) Close() error {
	return s.store.Close()
}
