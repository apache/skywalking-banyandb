package measure

import (
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

type measureSchema struct {
	measure  *databasev1.Measure
	fieldMap map[string]*fieldSpec
	common   *CommonSchema
}

func (m *measureSchema) Scope() tsdb.Entry {
	return tsdb.Entry(m.measure.Metadata.Name)
}

func (m *measureSchema) EntityList() []string {
	return m.common.EntityList()
}

func (m *measureSchema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexDefined(tagName)
}

func (m *measureSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexRuleDefined(indexRuleName)
}

func (m *measureSchema) CreateTagRef(tags ...[]*Tag) ([][]*TagRef, error) {
	return m.common.CreateRef(tags...)
}

func (m *measureSchema) CreateFieldRef(fields ...*Field) ([]*FieldRef, error) {
	fieldRefs := make([]*FieldRef, len(fields))
	for idx, field := range fields {
		if fs, ok := m.fieldMap[field.Name]; ok {
			fieldRefs[idx] = &FieldRef{field, fs}
		} else {
			return nil, errors.Wrap(ErrFieldNotDefined, field.Name)
		}
	}
	return fieldRefs, nil
}

func (m *measureSchema) ProjTags(refs ...[]*TagRef) Schema {
	if len(refs) == 0 {
		return nil
	}
	newSchema := &measureSchema{
		measure:  m.measure,
		common:   m.common.ProjTags(refs...),
		fieldMap: m.fieldMap,
	}
	return newSchema
}

func (m *measureSchema) ProjFields(fieldRefs ...*FieldRef) Schema {
	newFieldMap := make(map[string]*fieldSpec)
	i := 0
	for _, fr := range fieldRefs {
		if spec, ok := m.fieldMap[fr.field.Name]; ok {
			spec.FieldIdx = i
			newFieldMap[fr.field.Name] = spec
		}
		i++
	}
	return &measureSchema{
		measure:  m.measure,
		common:   m.common,
		fieldMap: newFieldMap,
	}
}

func (m *measureSchema) Equal(s2 Schema) bool {
	if other, ok := s2.(*measureSchema); ok {
		// TODO: add more equality checks
		return cmp.Equal(other.common.TagMap, m.common.TagMap)
	}
	return false
}

func (m *measureSchema) ShardNumber() uint32 {
	return m.common.ShardNumber()
}

// registerTag registers the tag spec with given tagFamilyIdx and tagIdx.
func (m *measureSchema) registerTag(tagFamilyIdx, tagIdx int, spec *databasev1.TagSpec) {
	m.common.registerTag(tagFamilyIdx, tagIdx, spec)
}

// registerField registers the field spec with given index.
func (m *measureSchema) registerField(fieldIdx int, spec *databasev1.FieldSpec) {
	m.fieldMap[spec.GetName()] = &fieldSpec{
		FieldIdx: fieldIdx,
		spec:     spec,
	}
}

func (m *measureSchema) TraceIDFieldName() string {
	// We don't have traceID for measure
	panic("implement me")
}
