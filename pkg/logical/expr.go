package logical

import (
	"errors"
	"fmt"
	"strconv"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

var (
	NoSuchField          = errors.New("no such field")
	NotSupporterBinaryOp = errors.New("not supported binary operator")
)

var _ Expr = (*FieldRef)(nil)

type FieldRef struct {
	fieldName string
}

func NewKeyRef(keyName string) Expr {
	return &FieldRef{fieldName: keyName}
}

func (ref FieldRef) String() string {
	return "#" + ref.fieldName
}

func (ref FieldRef) ToField(plan Plan) (types.Field, error) {
	schema, err := plan.Schema()
	if err != nil {
		return nil, err
	}
	for _, f := range schema.GetFields() {
		if f.Name() == ref.fieldName {
			return f, nil
		}
	}
	return nil, NoSuchField
}

var _ Expr = (*StringLit)(nil)

type StringLit struct {
	literal string
}

func (s *StringLit) String() string {
	return fmt.Sprintf("'%s'", s.literal)
}

func (s *StringLit) ToField(Plan) (types.Field, error) {
	return types.NewField(s.literal, types.STRING), nil
}

var _ Expr = (*Int64Lit)(nil)

type Int64Lit struct {
	literal int64
}

func (i *Int64Lit) String() string {
	return strconv.FormatInt(i.literal, 10)
}

func (i *Int64Lit) ToField(Plan) (types.Field, error) {
	return types.NewField(strconv.FormatInt(i.literal, 10), types.INT64), nil
}

var _ Expr = (*BinaryExpr)(nil)

type BinaryExpr struct {
	name  string
	op    apiv1.BinaryOp
	left  Expr
	right Expr
}

func (b BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", b.left, b.op.String(), b.right)
}

func (b BinaryExpr) ToField(Plan) (types.Field, error) {
	switch b.op {
	case apiv1.BinaryOpEQ, apiv1.BinaryOpNE, apiv1.BinaryOpLT, apiv1.BinaryOpGT, apiv1.BinaryOpGE, apiv1.BinaryOpHAVING, apiv1.BinaryOpNOT_HAVING:
		return types.NewField(b.name, types.BOOLEAN), nil
	default:
		return nil, NotSupporterBinaryOp
	}
}
