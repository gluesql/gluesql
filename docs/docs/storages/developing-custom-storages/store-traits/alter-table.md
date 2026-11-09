---
sidebar_position: 3
---

# AlterTable

`AlterTable` handles SQL ALTER TABLE statements. Its default methods use `Store` and `StoreMut`. Override them for storage-specific schema changes, such as updating secondary indexes.

It provides four methods:

1. `rename_schema`: Corresponds to the SQL statement `ALTER TABLE {table-name} RENAME TO {other-name};`. This method renames a schema.

2. `rename_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} RENAME COLUMN {col1} TO {col2};`. This method renames a column within a table.

3. `add_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} ADD COLUMN {col} {data-type} {constraints}`. This method adds a new column to a table with specified data type and constraints.

4. `drop_column`: Corresponds to the SQL statement `ALTER TABLE {table-name} DROP COLUMN {col}`. This method removes a column from a table.

```rust
pub trait AlterTable: Store + StoreMut {
    fn rename_schema(&mut self, table_name: &str, new_table_name: &str) -> Result<()>;

    fn rename_column(
        &mut self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
    ) -> Result<()>;

    fn add_column(&mut self, table_name: &str, column_def: &ColumnDef) -> Result<()>;

    fn drop_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        if_exists: bool,
    ) -> Result<()>;
}
```
