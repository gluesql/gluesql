---
sidebar_position: 10
---

# NativeQuery

`NativeQuery` executes native read queries prepared by a storage's custom planner.
Override `query_native` to run these queries through the storage's own engine.

The default method returns an unsupported-operation error. A storage without
native query support can keep this default:

```rust
use gluesql_core::store::NativeQuery;

impl NativeQuery for MyStorage {}
```

The trait provides one method:

```rust
pub type NativeRowIter<'a> = Box<dyn Iterator<Item = Result<Vec<Value>>> + 'a>;

pub trait NativeQuery {
    fn query_native<'a>(
        &'a self,
        query: &str,
        params: &[Value],
        output_types: &[DataType],
    ) -> Result<NativeRowIter<'a>>;
}
```

## Planning native queries

[SQLite Storage](../../supported-storages/sqlite-storage.md) implements both a
custom planner and `NativeQuery`. Its
[planner](https://github.com/gluesql/gluesql/blob/main/storages/sqlite-storage/src/planner.rs)
runs the common planning passes first, then applies a SQLite-specific pass. This
lets the native pass use the resolved schemas and required scan columns while
keeping access paths, such as primary-key lookups, selected by the common planner.

The [native filter pass](https://github.com/gluesql/gluesql/blob/main/storages/sqlite-storage/src/planner/native_filter.rs)
replaces eligible table scans with `SourcePlan::Native(NativeSourcePlan)`.
The native source carries:

- `query`: the query string in the backend's native syntax.
- `params`: the bound values, in the order expected by the query.
- `alias`: the source name used by subsequent GlueSQL operations.
- `columns`: ordered `NativeColumnPlan` entries, each with a logical column name
  and a GlueSQL `DataType`.

The executor passes `query`, `params`, and the column types to `query_native`.
Column names and the alias are used by GlueSQL; returned values are matched to
columns by position.

SQLite's [condition planner](https://github.com/gluesql/gluesql/blob/main/storages/sqlite-storage/src/planner/native_filter/condition.rs)
extracts supported AND terms and returns their SQL, bound values, and the
remaining expression. For a table defined as `Item (qty INT, name TEXT)`, consider:

```sql
SELECT name
FROM Item
WHERE qty > 10 AND LOWER(name) = 'apple';
```

The native query applies `qty > 10`. A GlueSQL Filter above that source evaluates
`LOWER(name) = 'apple'`, and the Project returns `name`. If no condition can be
delegated, the pass keeps the original plan. This decision belongs in planning;
the executor does not retry a failed native query through `Store`.

## Returned rows

See SQLite's [NativeQuery implementation](https://github.com/gluesql/gluesql/blob/main/storages/sqlite-storage/src/native_query.rs)
for the execution side. It prepares a read-only statement, checks the result
column count, encodes the bound values, and returns an iterator that decodes each
row into GlueSQL values.

Return one value for each entry in `output_types`, in the same order and with
the requested logical type. The column names describe how GlueSQL refers to
those positions; decoding uses the types supplied to `query_native`.

An empty `output_types` list still requires one empty value list per matching
row. SQLite uses `SELECT 1` for this case, and its iterator discards the dummy
column. This preserves row counts when later operations do not need source
column values.

The returned iterator may borrow the storage. It cannot borrow `query`, `params`,
or `output_types` beyond the method call. Prepare or copy any data needed during
iteration before returning. SQLite keeps the prepared statement in its
[row iterator](https://github.com/gluesql/gluesql/blob/main/storages/sqlite-storage/src/native_query/row_iter.rs)
and copies the output types needed to decode subsequent rows.
