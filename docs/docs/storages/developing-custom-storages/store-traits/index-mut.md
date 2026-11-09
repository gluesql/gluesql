---
sidebar_position: 8
---

# IndexMut

`IndexMut` creates and drops secondary indexes. Override its two methods to support CREATE INDEX and DROP INDEX; both return unsupported errors by default.

1. `create_index`: This method creates a new non-clustered index on the specified column with a given index name for the provided table. The index can be used to speed up queries involving the indexed column.

2. `drop_index`: This method removes a non-clustered index by the specified index name from the provided table. This can be useful when the index is no longer needed or needs to be updated to reflect changes in the data.

```rust
pub trait IndexMut {
    fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        column: &OrderByExpr,
    ) -> Result<()>;

    fn drop_index(&mut self, table_name: &str, index_name: &str) -> Result<()>;
}
```
