---
sidebar_position: 2
---

# StoreMut

While the `Store` trait is for supporting SELECT queries and reading data, the `StoreMut` trait is for modifying data. Implementing the `StoreMut` trait requires the implementation of the `Store` trait as well. By implementing both the `Store` and `StoreMut` traits, you can support most of the commonly used SQL statements. Additionally, you can use the Test Suite to utilize the integration test set provided by GlueSQL. Custom storage developers can verify their own Store & StoreMut implementations by checking if they pass all the tests provided in the Test Suite.

Here are the five methods required to implement the `StoreMut` trait:

1. `insert_schema`: This method is responsible for inserting a new schema into the storage system.

2. `delete_schema`: This method is for deleting a schema from the storage system using the provided table name.

3. `append_data`: This method appends a list of data rows to an existing table in the storage system.

4. `insert_data`: This method inserts a list of key-data row pairs into an existing table in the storage system.

5. `delete_data`: This method deletes a list of keys and their corresponding data rows from an existing table in the storage system.

```rust
/// By implementing `StoreMut` trait,
/// you can run `INSERT`, `CREATE TABLE`, `DELETE`, `UPDATE` and `DROP TABLE` queries.
#[async_trait(?Send)]
pub trait StoreMut {
    async fn insert_schema(&mut self, schema: &Schema) -> Result<()>;

    async fn delete_schema(&mut self, table_name: &str) -> Result<()>;

    async fn append_data(&mut self, table_name: &str, rows: Vec<DataRow>) -> Result<()>;

    async fn insert_data(&mut self, table_name: &str, rows: Vec<(Key, DataRow)>) -> Result<()>;

    async fn delete_data(&mut self, table_name: &str, keys: Vec<Key>) -> Result<()>;
}
```