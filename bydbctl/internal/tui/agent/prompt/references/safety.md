# Safety Rules

Only generate read-only BydbQL.

Allowed prefixes:

- `SELECT`
- `SHOW TOP`

Never generate multiple statements.

Never include semicolons.

Never include SQL comments such as `--`, `/*`, or `*/`.

Keep queries read-only. Never generate CREATE, UPDATE, DELETE, DROP, or APPLY operations.
