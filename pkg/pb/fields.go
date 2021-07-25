package pb

import (
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
)

func Transform(entityValue *v1.EntityValue, fieldIndexes []FieldEntry) []*v1.TypedPair {
	typedPairs := make([]*v1.TypedPair, 0)
	if fieldIndexes != nil {
		// copy selected fields
		for _, fieldIndex := range fieldIndexes {
			key, idx := fieldIndex.Key, fieldIndex.Index
			if idx >= len(entityValue.GetFields()) {
				// skip
				continue
			}
			f := entityValue.GetFields()[idx]
			switch v := f.GetValueType().(type) {
			case *v1.Field_Str:
				typedPairs = append(typedPairs, buildPair(key, v.Str.GetValue()))
			case *v1.Field_StrArray:
				typedPairs = append(typedPairs, buildPair(key, v.StrArray.GetValue()))
			case *v1.Field_Int:
				typedPairs = append(typedPairs, buildPair(key, v.Int.GetValue()))
			case *v1.Field_IntArray:
				typedPairs = append(typedPairs, buildPair(key, v.IntArray.GetValue()))
			case *v1.Field_Null:
			}
		}
	} else {
		panic("what is the key?")
	}
	return typedPairs
}

type FieldEntry struct {
	Key   string
	Index int
}
