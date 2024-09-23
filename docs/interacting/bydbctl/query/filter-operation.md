# Filter Operation

Filter operation is a part of the query configuration. It is used to filter the data based on the given condition for [Stream](stream.md) and [Measure](measure.md) queries.

The condition is a combination of the tag name, operation, and value. 
The operation's root is Criteria which is defined in the [API Reference](../../../api-reference.md#criteria).

The following are the examples of filter operations:

## [Condition.BinaryOp](../../../api-reference.md#conditionbinaryop)

### EQ, NE, LT, GT, LE and GE
EQ, NE, LT, GT, LE and GE, only one operand should be given, i.e. one-to-one relationship.

```shell
criteria:
  condition:
    name: "entity_id"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "entity_1"
```

### IN and NOT_IN
HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship.

```shell
criteria:
  condition:
    name: "entity_id"
    op: "BINARY_OP_IN"
    value:
      str_array:
        value: ["entity_1", "entity_2", "unknown"]
```

### HAVING and NOT_HAVING
HAVING and NOT_HAVING allow multi-value to be the operand such as array/vector, i.e. one-to-many relationship. For example, "keyA" contains "valueA" and "valueB"

```shell
criteria:
  condition:
    name: "extended_tags"
    op: "BINARY_OP_HAVING"
    value:
      strArray:
        value: ["c", "b"]
```

### MATCH
MATCH performances a full-text search if the tag is analyzed.
The string value applies to the same analyzer as the tag, but string array value does not.
Each item in a string array is seen as a token instead of a query expression.

How to set the analyzer for a tag can find in the [IndexRules](../schema/index-rule.md).

```shell
criteria:
  condition:
    name: "name"
    op: "BINARY_OP_MATCH"
    value:
      str:
        value: "us"
```

You can set a `match_option` to control the behavior of the match operation. The following are the available options:

- `analyzer`: The analyzer to use for the match operation. If not set, the analyzer defined in the index rule will be used. Available options are defined in the [IndexRules](../schema/index-rule.md).
- `operator`: The operator to use for the match operation. The default value is `OPERATOR_OR`. Available options are `OPERATOR_OR` and `OPERATOR_AND`.

If you want to use a different analyzer and operator, you can set the `match_option` as follows:

```shell
criteria:
  condition:
    name: "name"
    op: "BINARY_OP_MATCH"
    value:
      str:
        value: "service-1"
    match_option:
      analyzer: "url"
      operator: "OPERATOR_AND"
```

Considering the data with the following tags:

```shell
{
  "name": "service-1"
}
{
  "name": "service-2"
}
```

The above query will return the data with the tag `name` that contains both `service` and `1`, which is `service-1`.

If you set the `operator` to `OPERATOR_OR`, the query will return the data with the tag `name` that contains either `service` or `1`, which is `service-1` and `service-2`.

## [LogicalExpression.LogicalOp](../../../api-reference.md#logicalexpressionlogicalop)
Logical operation is used to combine multiple conditions.

### AND, OR
The following example queries the data where the `id` is `1` and the `service_id` is `service_1`

```shell
criteria:
  le:
    op: "LOGICAL_OP_AND"
    right:
      condition:
        name: "id"
        op: "BINARY_OP_EQ"
        value:
          str:
            value: "1"
    left:
      condition:
        name: "service_id"
        op: "BINARY_OP_EQ"
        value:
          str:
            value: "service_1"
```
