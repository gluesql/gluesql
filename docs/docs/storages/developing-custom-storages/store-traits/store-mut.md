---
sidebar_position: 2
---

# StoreMut

`StoreMut` writes schemas and rows for CREATE TABLE, INSERT, UPDATE, DELETE, and DROP TABLE. Override the methods your storage supports. The defaults return unsupported errors, which a read-only adapter can keep.

It provides five methods:

1. `insert_schema`: This method is responsible for inserting a new schema into the storage system.

2. `delete_schema`: This method is for deleting a schema from the storage system using the provided table name.

3. `append_data`: This method appends a list of data rows to an existing table in the storage system.

4. `insert_data`: This method inserts a list of key-data row pairs into an existing table in the storage system.

5. `delete_data`: This method deletes a list of keys and their corresponding data rows from an existing table in the storage system.

```rust
/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
pub trait StoreMut {
    fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    fn append_data(&mut self, table_name: &str, rows: Vec<Vec<Value>>) -> Result<()>;

    fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, Vec<Value>)>) -> Result<()>;

    fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}
```
