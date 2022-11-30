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
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pkg/errors"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	ErrTagNotDefined              = errors.New("tag is not defined")
	ErrFieldNotDefined            = errors.New("field is not defined")
	ErrUnsupportedConditionOp     = errors.New("unsupported condition operation")
	ErrUnsupportedConditionValue  = errors.New("unsupported condition value type")
	ErrInvalidCriteriaType        = errors.New("invalid criteria type")
	ErrIncompatibleQueryCondition = errors.New("incompatible query condition type")
	ErrIndexNotDefined            = errors.New("index is not define for the tag")
	ErrMultipleGlobalIndexes      = errors.New("multiple global indexes are not supported")
	ErrInvalidData                = errors.New("data is invalid")

	nullTag = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
)

type (
	SeekerBuilder func(builder tsdb.SeekerBuilder)
	Comparator    func(a, b tsdb.Item) bool
)

func CreateComparator(sortDirection modelv1.Sort) Comparator {
	return func(a, b tsdb.Item) bool {
		comp := bytes.Compare(a.SortedField(), b.SortedField())
		if sortDirection == modelv1.Sort_SORT_DESC {
			return comp == 1
		}
		return comp == -1
	}
}

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
		tags := make([]*modelv1.Tag, len(refs))
		familyName := refs[0].Tag.GetFamilyName()
		parsedTagFamily, err := ec.ParseTagFamily(familyName, item)
		if err != nil {
			return nil, errors.WithMessage(err, "parse projection")
		}
		if len(refs) > len(parsedTagFamily.Tags) {
			return nil, errors.Wrapf(ErrInvalidData,
				"the number of tags %d in %s is less then expected %d",
				len(parsedTagFamily.Tags), familyName, len(refs))
		}
		for j, ref := range refs {
			if len(parsedTagFamily.GetTags()) > ref.Spec.TagIdx {
				tags[j] = parsedTagFamily.GetTags()[ref.Spec.TagIdx]
			} else {
				tags[j] = &modelv1.Tag{Key: ref.Tag.name, Value: nullTag}
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
func ExecuteForShard(l *logger.Logger, series tsdb.SeriesList, timeRange timestamp.TimeRange,
	builders ...SeekerBuilder,
) ([]tsdb.Iterator, []io.Closer, error) {
	var itersInShard []tsdb.Iterator
	var closers []io.Closer
	for _, seriesFound := range series {
		itersInSeries, err := func() ([]tsdb.Iterator, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sp, errInner := seriesFound.Span(context.WithValue(ctx, logger.ContextKey, l), timeRange)
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
			return nil, nil, err
		}
		if len(itersInSeries) > 0 {
			itersInShard = append(itersInShard, itersInSeries...)
		}
	}
	return itersInShard, closers, nil
}

var DefaultLimit uint32 = 20

type Tag struct {
	familyName, name string
}

func NewTag(family, name string) *Tag {
	return &Tag{
		familyName: family,
		name:       name,
	}
}

// NewTags create an array of Tag within a TagFamily
func NewTags(family string, tagNames ...string) []*Tag {
	tags := make([]*Tag, len(tagNames))
	for i, name := range tagNames {
		tags[i] = NewTag(family, name)
	}
	return tags
}

// GetCompoundName is only used for error message
func (t *Tag) GetCompoundName() string {
	return t.familyName + ":" + t.name
}

func (t *Tag) GetTagName() string {
	return t.name
}

func (t *Tag) GetFamilyName() string {
	return t.familyName
}

type Field struct {
	Name string
}

func NewField(name string) *Field {
	return &Field{Name: name}
}

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
