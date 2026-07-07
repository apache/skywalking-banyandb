# Safety Rules

Only generate read-only BydbQL.

Allowed prefixes:

- `SELECT`
- `SHOW TOP`

Never generate multiple statements.

Never include semicolons.

Never include SQL comments such as `--`, `/*`, or `*/`.

Validate with `validate_bydbql` before execution.

Parse-only validation does not prove that groups, resources, tags, fields, or index rules exist. Use `list_groups_schemas` when names are missing, ambiguous, or rejected by execution.
