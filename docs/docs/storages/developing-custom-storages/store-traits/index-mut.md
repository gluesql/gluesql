---
sidebar_position: 8
---

# IndexMut

The `IndexMut` trait, when implemented along with the `Index` trait, allows custom storage developers to provide users with the ability to create, use, and delete non-clustered indexes. Implementing the `IndexMut` trait enhances the storage system's capabilities by providing support for dynamic index management.

Non-clustered indexes can improve query performance by reducing the amount of data that needs to be read during search operations. Users can create multiple non-clustered indexes tailored to their specific use cases, providing a more efficient and optimized experience when working with large datasets.

The `IndexMut` trait requires the implementation of two methods:

1. `create_index`: This method creates a new non-clustered index on the specified column with a given index name for the provided table. The index can be used to speed up queries involving the indexed column.

2. `drop_index`: This method removes a non-clustered index by the specified index name from the provided table. This can be useful when the index is no longer needed or needs to be updated to reflect changes in the data.

```rust
#[async_trait]
pub trait IndexMut {
    async fn create_index(
        &mut self,
        _table_name: &str,
        _index_name: &str,
        _column: &OrderByExpr,
    ) -> Result<()>;

    async fn drop_index(&mut self, _table_name: &str, _index_name: &str) -> Result<()>;
}
```

By implementing both the `Index` and `IndexMut` traits, custom storage developers can provide users with the ability to manage non-clustered indexes according to their specific needs, improving overall query performance and providing a more tailored experience.
