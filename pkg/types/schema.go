package types

//go:generate mockery --name Schema --output ../internal/mocks
type Schema interface {
	Select(names ...string) Schema
	GetFields() []Field
}
