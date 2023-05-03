---
sidebar_position: 7
---

# Index

The `Index` trait is designed to support non-clustered indexes. If you only need to support pre-built non-clustered indexes, implementing the `Index` trait without the `IndexMut` trait is sufficient. Note that clustered indexes (PRIMARY KEY) are automatically supported by the `Store` & `StoreMut` implementations. The `Index` trait is specifically for non-clustered index support.

Currently, GlueSQL's query planner only supports a logical planner, so the performance of finding non-clustered indexes is not optimal yet, but it is being improved. If you want to use non-clustered indexes more precisely, using the AST Builder to directly specify the index you want to use can be a good approach.

A brief explanation of non-clustered and clustered indexes:

- Non-clustered index: A non-clustered index is an index that doesn't affect the physical ordering of the data rows in the table. Instead, it maintains a separate data structure that contains a reference to the actual data rows, allowing for faster search operations without rearranging the data itself.

- Clustered index: A clustered index determines the physical order of the data in the table. In other words, the data rows are stored on disk in the same order as the index. There can be only one clustered index per table, which is usually defined by the PRIMARY KEY constraint.

There is one method to implement for the `Index` trait:

1. `scan_indexed_data`: This method retrieves indexed data from the storage system using the provided table name, index name, sorting order, and comparison value.

```rust
#[async_trait(?Send)]
pub trait Index {
    async fn scan_indexed_data(
        &self,
        _table_name: &str,
        _index_name: &str,
        _asc: Option<bool>,
        _cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter>;
}
```