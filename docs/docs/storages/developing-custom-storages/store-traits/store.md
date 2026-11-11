---
sidebar_position: 1
---

# Store

`Store` reads schemas and rows for SELECT queries. Implement its four methods to query data from a database, file, or API.

Here are the four methods required to implement the `Store` trait:

1. `fetch_schema`: This method is responsible for fetching a schema for a given table name. It returns an optional schema if the table exists.

2. `fetch_all_schemas`: This method fetches all the schemas from the storage system. It returns a vector of schemas.

3. `fetch_data`: This method fetches a specific data row from the storage system using the provided table name and key. It returns an optional data row if the key exists in the table.

4. `scan_data`: This method scans the rows in a table and returns the requested column values with each row's key.

```rust
pub type RowIter<'a> = Box<dyn Iterator<Item = Result<(Key, Vec<Value>)>> + 'a>;
pub type ScanColumn = (String, usize);

#[derive(Debug, Clone, Copy)]
pub enum ScanColumns<'a> {
    All,
    Selected(&'a [ScanColumn]),
}

/// By implementing `Store` trait, you can run `SELECT` query.
pub trait Store {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>>;

    fn scan_data<'a>(
        &'a self,
        table_name: &str,
        columns: ScanColumns<'_>,
    ) -> Result<RowIter<'a>>;
}
```

Each `ScanColumn` contains an original column name and its zero-based position
in the full table schema. Supply selected columns in strictly increasing index
order, without duplicates. Return their values in that same schema order. For
example, given `[id, name, payload, price]`, the request
`[("name", 1), ("price", 3)]` returns `[name, price]` for each row.

`ScanColumns::All` requests all columns. An empty request,
`ScanColumns::Selected(&[])`, returns an empty value vector for every row.
Column selection must preserve row keys and row counts; a storage may read a
primary key internally without returning its value unless requested.
Schemaless tables use `ScanColumns::All` to return complete documents.

The request is not borrowed by the returned iterator. Copy any positions or
reader projection needed during iteration. The supplied names and positions
identify the same columns, so resolving them does not require another schema
lookup.
