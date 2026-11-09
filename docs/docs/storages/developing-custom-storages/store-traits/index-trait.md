---
sidebar_position: 7
---

# Index

`Index` reads rows using the secondary index selected in the query plan. Implement `scan_indexed_data` to support index lookups; the default returns an unsupported error.

Primary-key lookups use `Store` and do not need secondary-index support.

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
