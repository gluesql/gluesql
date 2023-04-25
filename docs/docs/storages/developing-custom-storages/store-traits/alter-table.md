---
sidebar_position: 3
---

# AlterTable

The `AlterTable` trait corresponds to the SQL ALTER TABLE statement and is used for modifying existing schemas. It is not necessary to implement the `AlterTable` trait. If you are dealing with data that is difficult to modify schema-wise or schemaless data, there is no need to implement the `AlterTable` trait. It is an optional trait that custom storage developers can choose to implement.

Similar to the `Store` & `StoreMut` combination, if you implement the `AlterTable` trait, you can use additional tests in the Test Suite to verify your implementation. There are currently four types of methods supported by `AlterTable`:

1. `rename_schema`: Corresponds to the SQL statement `ALTER TABLE {table-name} RENAME TO {other-name};`. This method renames a schema.

2. `rename_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} RENAME COLUMN {col1} TO {col2};`. This method renames a column within a table.

3. `add_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} ADD COLUMN {col} {data-type} {constraints}`. This method adds a new column to a table with specified data type and constraints.

4. `drop_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} DROP COLUMN {col}`. This method removes a column from a table.

```rust
#[async_trait(?Send)]
pub trait AlterTable {
    async fn rename_schema(&mut self, _table_name: &str, _new_table_name: &str) -> Result<()>;

    async fn rename_column(&mut self,
        _table_name: &str,
        _old_column_name: &str,
        _new_column_name: &str,
    ) -> Result<()>;

    async fn add_column(&mut self, _table_name: &str, _column_def: &ColumnDef) -> Result<()>;

    async fn drop_column(
        &mut self,
        _table_name: &str,
        _column_name: &str,
        _if_exists: bool,
    ) -> Result<()>;
}
```
