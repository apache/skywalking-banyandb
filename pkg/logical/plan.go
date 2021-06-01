package logical

import (
	"fmt"
	"strings"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

var _ Plan = (*Selection)(nil)

type Selection struct {
	input Plan
	expr  Expr
}

func (s *Selection) String() string {
	return fmt.Sprintf("Selection: %s", s.expr.String())
}

func (s *Selection) Schema() (types.Schema, error) {
	return s.input.Schema()
}

func (s *Selection) Children() []Plan {
	return []Plan{s.input}
}

var _ Plan = (*Projection)(nil)

type Projection struct {
	input Plan
	exprs []Expr
}

func (p *Projection) String() string {
	var expressStr []string
	for _, e := range p.exprs {
		expressStr = append(expressStr, e.String())
	}
	return "Projection: " + strings.Join(expressStr, ", ")
}

func (p *Projection) Schema() (types.Schema, error) {
	var fields []types.Field
	for _, e := range p.exprs {
		f, err := e.ToField(p.input)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return types.NewSchema(fields...), nil
}

func (p *Projection) Children() []Plan {
	return []Plan{p.input}
}

var _ Plan = (*Scan)(nil)

type Scan struct {
	metadata   apiv1.Metadata
	projection []string
}

func (s *Scan) String() string {
	if len(s.projection) == 0 {
		return fmt.Sprintf("Scan: Metadata{group=%s, name=%s}; projection=None", s.metadata.Group(), s.metadata.Name())
	} else {
		return fmt.Sprintf("Scan: Metadata{group=%s, name=%s}; projection=%v", s.metadata.Group(), s.metadata.Name(), s.projection)
	}
}

func (s *Scan) Schema() (types.Schema, error) {
	panic("implement me")
}

func (s *Scan) Children() []Plan {
	return []Plan{}
}
