package types

var _ Field = (*field)(nil)

type FieldType uint8

const (
	STRING FieldType = iota
	INT64
	BOOLEAN
)

//go:generate mockery --name Field --output ../internal/mocks
type Field interface {
	Name() string
	Type() FieldType
}

type field struct {
	name string
	typ  FieldType
}

func (f field) Name() string {
	return f.name
}

func (f field) Type() FieldType {
	return f.typ
}

func NewField(fieldName string, typ FieldType) Field {
	return &field{fieldName, typ}
}
