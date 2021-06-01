package logical

import (
	"errors"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	flatbuffers "github.com/google/flatbuffers/go"
)

var (
	InvalidPairType = errors.New("invalid pair type")
)

func ComposeLogicalPlan(criteria apiv1.EntityCriteria) (Plan, error) {
	metadata := criteria.Metatdata(nil)
	timeRange := criteria.TimestampNanoseconds(nil)
	begin, end := timeRange.Begin(), timeRange.End()
	scan := NewScan(metadata, begin, end)

	var filterExprs []Expr
	for i := 0; i < criteria.FieldsLength(); i++ {
		var pairQuery apiv1.PairQuery
		if ok := criteria.Fields(&pairQuery, i); ok {
			condition := pairQuery.Condition(nil)
			unionTable := new(flatbuffers.Table)

			if condition.Pair(unionTable) {
				pairType := condition.PairType()
				if pairType == apiv1.TypedPairIntPair {
					unionIntPair := new(apiv1.IntPair)
					unionIntPair.Init(unionTable.Bytes, unionTable.Pos)
					key := string(unionIntPair.Key())
					// TODO: support array
					value := unionIntPair.Values(0)
					f := operatorFactory[pairQuery.Op()](NewFieldRef(key), Long(value))
					filterExprs = append(filterExprs, f)
				} else if pairType == apiv1.TypedPairStrPair {
					unionStrPair := new(apiv1.StrPair)
					unionStrPair.Init(unionTable.Bytes, unionTable.Pos)
					key := string(unionStrPair.Key())
					// TODO: support array
					value := string(unionStrPair.Values(0))
					f := operatorFactory[pairQuery.Op()](NewFieldRef(key), Str(value))
					filterExprs = append(filterExprs, f)
				} else {
					return nil, InvalidPairType
				}
			}
		}
	}

	selection := NewSelection(scan, filterExprs...)

	var projectionList []Expr
	keys := criteria.Projection(nil)
	for i := 0; i < keys.KeyNamesLength(); i++ {
		keyName := string(keys.KeyNames(i))
		projectionList = append(projectionList, NewFieldRef(keyName))
	}

	projection := NewProjection(selection, projectionList...)

	return projection, nil
}
