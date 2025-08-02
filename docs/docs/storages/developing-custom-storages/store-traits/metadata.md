---
sidebar_position: 9
---

# Metadata

The `Metadata` trait is an optional implementation for providing additional metadata support in GlueSQL. GlueSQL does not enforce any specific metadata implementation, allowing custom storage developers to decide which type of metadata, such as create time, modify time, etc., they want to provide.

Implementing the `Metadata` trait can be beneficial in cases where users need to access and manage metadata related to tables, columns, or other database objects. This can help users understand the structure and properties of their data, ensuring proper management and organization.

Currently, the `Metadata` trait supports the `scan_table_meta` method for retrieving table metadata. The metadata provided by the storage can be queried using the data dictionary table `GLUE_TABLES`.

```rust
type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, HashMap<String, Value>)>>>;

#[async_trait]
pub trait Metadata {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
```

By implementing the `Metadata` trait, custom storage developers can provide users with a way to access and manage metadata related to various database objects. This can be particularly useful in situations where users need to understand the properties of their data or maintain a well-organized database structure.
