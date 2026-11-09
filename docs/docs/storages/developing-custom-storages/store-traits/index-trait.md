---
sidebar_position: 7
---

# Index

`Index` reads rows through secondary indexes. Implement `scan_indexed_data` to support index lookups; the default returns an unsupported error.

Primary-key lookups use `Store` and do not need secondary-index support.

A storage can select indexes in its `Planner`. Redb's planner includes secondary-index planning. You can also choose an index through the Query Builder.

It provides one method:

1. `scan_indexed_data`: This method retrieves indexed data from the storage system using the provided table name, index name, sorting order, and comparison value.

```rust
pub trait Index {
    fn scan_indexed_data<'a>(
        &'a self,
        table_name: &str,
        index_name: &str,
        asc: Option<bool>,
        cmp_value: Option<(&IndexOperator, Value)>,
    ) -> Result<RowIter<'a>>;
}
```
