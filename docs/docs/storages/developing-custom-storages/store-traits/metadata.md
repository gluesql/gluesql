---
sidebar_position: 9
---

# Metadata

The `Metadata` trait lets a storage expose additional information about its tables. GlueSQL does not require a fixed set of properties; each storage decides what to provide, such as creation or modification times.

Implement `scan_table_meta` to return the metadata for each table. Users can query these properties through the `GLUE_OBJECTS` data dictionary table.

```rust
type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, BTreeMap<String, Value>)>>>;

pub trait Metadata {
    fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
```
