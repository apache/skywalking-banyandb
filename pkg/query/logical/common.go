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

package logical

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	errTagNotDefined             = errors.New("tag is not defined")
	errUnsupportedConditionOp    = errors.New("unsupported condition operation")
	errUnsupportedConditionValue = errors.New("unsupported condition value type")
	errInvalidCriteriaType       = errors.New("invalid criteria type")
	errIndexNotDefined           = errors.New("index is not define for the tag")

	nullTag = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
)

type (
	// SeekerBuilder wraps the execution of tsdb.SeekerBuilder.
	// TODO:// we could have a chance to remove this wrapper.
	SeekerBuilder func(builder tsdb.SeekerBuilder)
)

// ProjectItem parses the item within the StreamExecutionContext.
// projectionFieldRefs must be prepared before calling this method, projectionFieldRefs should be a list of
// tag list where the inner list must exist in the same tag family.
// Strict order can be guaranteed in the result.
func ProjectItem(ec executor.ExecutionContext, item tsdb.Item, projectionFieldRefs [][]*TagRef) ([]*modelv1.TagFamily, error) {
	tagFamily := make([]*modelv1.TagFamily, len(projectionFieldRefs))
	for i, refs := range projectionFieldRefs {
		if len(refs) == 0 {
			continue
		}
		familyName := refs[0].Tag.getFamilyName()
		parsedTagFamily, err := ec.ParseTagFamily(familyName, item)
		if err != nil {
			return nil, errors.WithMessage(err, "parse projection")
		}

		parsedTagSize := len(parsedTagFamily.GetTags())
		tagRefSize := len(refs)

		// Determine maximum size for creating the tags slice
		maxSize := tagRefSize
		if parsedTagSize < tagRefSize {
			maxSize = parsedTagSize
		}

		tags := make([]*modelv1.Tag, maxSize)

		for j, ref := range refs {
			if parsedTagSize > ref.Spec.TagIdx {
				tags[j] = parsedTagFamily.GetTags()[ref.Spec.TagIdx]
			} else if j < parsedTagSize {
				tags[j] = &modelv1.Tag{Key: ref.Tag.name, Value: nullTag}
			} else {
				break
			}
		}

		tagFamily[i] = &modelv1.TagFamily{
			Name: familyName,
			Tags: tags,
		}
	}

	return tagFamily, nil
}

// ExecuteForShard fetches elements from series within a single shard. A list of series must be prepared in advanced
// with the help of Entity. The result is a list of element set, where the order of inner list is kept
// as what the users specify in the seekerBuilder.
// This method is used by the underlying tableScan and localIndexScan plans.
func ExecuteForShard(ctx context.Context, l *logger.Logger, series tsdb.SeriesList, timeRange timestamp.TimeRange,
	builders ...SeekerBuilder,
) ([]tsdb.Iterator, []io.Closer, error) {
	var itersInShard []tsdb.Iterator
	var closers []io.Closer
	for _, seriesFound := range series {
		itersInSeries, err := func() ([]tsdb.Iterator, error) {
			ctxSeries, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			sp, errInner := seriesFound.Span(context.WithValue(ctxSeries, logger.ContextKey, l), timeRange)
			if errInner != nil {
				if errors.Is(errInner, tsdb.ErrEmptySeriesSpan) {
					return nil, nil
				}
				return nil, errInner
			}
			closers = append(closers, sp)
			b := sp.SeekerBuilder()
			for _, builder := range builders {
				builder(b)
			}
			seeker, errInner := b.Build()
			if errInner != nil {
				return nil, errInner
			}
			iters, errInner := seeker.Seek()
			if errInner != nil {
				return nil, errInner
			}
			return iters, nil
		}()
		if err != nil {
			if len(closers) > 0 {
				for _, closer := range closers {
					err = multierr.Append(err, closer.Close())
				}
			}
			return nil, nil, err
		}
		if len(itersInSeries) > 0 {
			itersInShard = append(itersInShard, itersInSeries...)
		}
	}
	return itersInShard, closers, nil
}

// Tag represents the combination of  tag family and tag name.
// It's a tag's identity.
type Tag struct {
	familyName, name string
}

// NewTag return a new Tag.
func NewTag(family, name string) *Tag {
	return &Tag{
		familyName: family,
		name:       name,
	}
}

// NewTags create an array of Tag within a TagFamily.
func NewTags(family string, tagNames ...string) []*Tag {
	tags := make([]*Tag, len(tagNames))
	for i, name := range tagNames {
		tags[i] = NewTag(family, name)
	}
	return tags
}

// GetCompoundName is only used for error message.
func (t *Tag) GetCompoundName() string {
	return t.familyName + ":" + t.name
}

func (t *Tag) getTagName() string {
	return t.name
}

func (t *Tag) getFamilyName() string {
	return t.familyName
}

// ToTags converts a projection spec to Tag sets.
func ToTags(projection *modelv1.TagProjection) [][]*Tag {
	projTags := make([][]*Tag, len(projection.GetTagFamilies()))
	for i, tagFamily := range projection.GetTagFamilies() {
		var projTagInFamily []*Tag
		for _, tagName := range tagFamily.GetTags() {
			projTagInFamily = append(projTagInFamily, NewTag(tagFamily.GetName(), tagName))
		}
		projTags[i] = projTagInFamily
	}
	return projTags
}

// Field identity a field in a measure.
type Field struct {
	Name string
}

// NewField return a new Field.
func NewField(name string) *Field {
	return &Field{Name: name}
}

// StringSlicesEqual reports whether a and b are the same length and contain the same strings.
// A nil argument is equivalent to an empty slice.
func StringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// NewItemIter returns a ItemIterator which mergers several tsdb.Iterator by input sorting order.
func NewItemIter(iters []tsdb.Iterator, s modelv1.Sort) sort.Iterator[tsdb.Item] {
	var ii []sort.Iterator[tsdb.Item]
	for _, iter := range iters {
		ii = append(ii, iter)
	}
	if s == modelv1.Sort_SORT_DESC {
		return sort.NewItemIter[tsdb.Item](ii, true)
	}
	return sort.NewItemIter[tsdb.Item](ii, false)
}
