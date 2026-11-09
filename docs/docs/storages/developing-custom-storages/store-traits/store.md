---
sidebar_position: 1
---

# Store

`Store` reads schemas and rows for SELECT queries. Implement its four methods to query data from a database, file, or API.

Here are the four methods required to implement the `Store` trait:

1. `fetch_schema`: This method is responsible for fetching a schema for a given table name. It returns an optional schema if the table exists.

2. `fetch_all_schemas`: This method fetches all the schemas from the storage system. It returns a vector of schemas.

3. `fetch_data`: This method fetches a specific data row from the storage system using the provided table name and key. It returns an optional data row if the key exists in the table.

4. `scan_data`: This method is used to scan all the data rows in a table. It returns an iterator over the rows in the specified table.

```rust
pub type RowIter<'a> = Box<dyn Iterator<Item = Result<(Key, Vec<Value>)>> + 'a>;

/// By implementing `Store` trait, you can run `SELECT` query.
pub trait Store {
    fn fetch_schema(&self, table_name: &str) -> Result<Option<Schema>>;

    fn fetch_all_schemas(&self) -> Result<Vec<Schema>>;

    fn fetch_data(&self, table_name: &str, key: &Key) -> Result<Option<Vec<Value>>>;

    fn scan_data<'a>(&'a self, table_name: &str) -> Result<RowIter<'a>>;
}
```
