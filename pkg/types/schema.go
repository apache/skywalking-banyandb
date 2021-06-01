package types

var _ Schema = (*schema)(nil)

type schema struct {
	fields []Field
}

func (s schema) Select(names ...string) Schema {
	var filteredFields []Field
	namesMap := make(map[string]struct{})
	for _, name := range names {
		namesMap[name] = struct{}{}
	}
	for _, f := range s.fields {
		if _, ok := namesMap[f.Name()]; ok {
			filteredFields = append(filteredFields, f)
		}
	}
	return NewSchema(filteredFields...)
}

func (s schema) GetFields() []Field {
	return s.fields
}

func NewSchema(fields ...Field) Schema {
	return &schema{
		fields: fields,
	}
}

//go:generate mockery --name Schema --output ../internal/mocks
type Schema interface {
	Select(names ...string) Schema
	GetFields() []Field
}
