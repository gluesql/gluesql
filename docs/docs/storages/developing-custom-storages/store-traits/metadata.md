---
sidebar_position: 9
---

# Metadata

`Metadata::scan_table_meta` returns table properties such as creation and modification times. SQL queries can read these properties through `GLUE_TABLES`. The default returns an empty iterator.

```rust
type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, BTreeMap<String, Value>)>>>;

pub trait Metadata {
    fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
```
