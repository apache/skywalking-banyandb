package measure

import (
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"

	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
)

type measureSchema struct {
	measure  *databasev1.Measure
	fieldMap map[string]*logical.FieldSpec
	common   *logical.CommonSchema
}

func (m *measureSchema) Scope() tsdb.Entry {
	return tsdb.Entry(m.measure.Metadata.Name)
}

func (m *measureSchema) EntityList() []string {
	return m.common.EntityList
}

func (m *measureSchema) IndexDefined(tagName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexDefined(tagName)
}

func (m *measureSchema) IndexRuleDefined(indexRuleName string) (bool, *databasev1.IndexRule) {
	return m.common.IndexRuleDefined(indexRuleName)
}

func (m *measureSchema) CreateTagRef(tags ...[]*logical.Tag) ([][]*logical.TagRef, error) {
	return m.common.CreateRef(tags...)
}

func (m *measureSchema) CreateFieldRef(fields ...*logical.Field) ([]*logical.FieldRef, error) {
	fieldRefs := make([]*logical.FieldRef, len(fields))
	for idx, field := range fields {
		if fs, ok := m.fieldMap[field.Name]; ok {
			fieldRefs[idx] = &logical.FieldRef{field, fs}
		} else {
			return nil, errors.Wrap(logical.ErrFieldNotDefined, field.Name)
		}
	}
	return fieldRefs, nil
}

func (m *measureSchema) ProjTags(refs ...[]*logical.TagRef) logical.Schema {
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

func (m *measureSchema) ProjFields(fieldRefs ...*logical.FieldRef) logical.Schema {
	newFieldMap := make(map[string]*logical.FieldSpec)
	i := 0
	for _, fr := range fieldRefs {
		if spec, ok := m.fieldMap[fr.Field.Name]; ok {
			spec.FieldIdx = i
			newFieldMap[fr.Field.Name] = spec
		}
		i++
	}
	return &measureSchema{
		measure:  m.measure,
		common:   m.common,
		fieldMap: newFieldMap,
	}
}

func (m *measureSchema) Equal(s2 logical.Schema) bool {
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
	m.common.RegisterTag(tagFamilyIdx, tagIdx, spec)
}

// registerField registers the field spec with given index.
func (m *measureSchema) registerField(fieldIdx int, spec *databasev1.FieldSpec) {
	m.fieldMap[spec.GetName()] = &logical.FieldSpec{
		FieldIdx: fieldIdx,
		Spec:     spec,
	}
}

func (m *measureSchema) TraceIDFieldName() string {
	// We don't have traceID for measure
	panic("implement me")
}
