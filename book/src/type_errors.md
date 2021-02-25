# Type Errors

In current integration test system, there should be no way to generate `Conflict-` or `Unreachable-`.

## `Conflict-` type errors exist because of storage level data conflict.

If users only manipulate data using `GlueSQL` and that is properly executed, then that is not possible to see `Conflict-` type errors.
One of the cases which users can see `Conflict-` errors are.. when user manipulate the storage data manually without accessing `GlueSQL` library and the modified data structure does not fit to `GlueSQL`.

e.g. `GlueSQL` fetched schema data and schema has a column with FLOAT UNIQUE
In normal use cases, it should be filtered out when user tried to create table with FLOAT UNIQUE.

## `Unreachable-` type errors are mainly because of parser.

Theoretically if the parsed AST is exactly as same as SQL, then we may not need `Unreachable-` errors.
However, from the point of view from `GlueSQL`, it looks like AST is superset of SQL.
Some ASTs don't look possible to generate from SQL.
Then, sure we also don't need to write codes for unreachable AST.
It is `Unreachable-`'s main role.

Other use cases are... when it is too hard to deal with hidden errors.
Good to avoid use `Unreachable-` for hidden errors, but if we get too stress from this, then use this.
It is not ideal, but it is better than giving up.
