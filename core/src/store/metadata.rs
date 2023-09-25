use {
    crate::{prelude::Value, result::Result},
    async_trait::async_trait,
    std::{collections::HashMap, iter::empty},
};

type ObjectName = String;
/// Yield sets of column name and type pairs for tables in your database.
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, HashMap<String, Value>)>>>;

/// Enables the retrieval of column names and types of all tables of database.
#[async_trait(?Send)]
pub trait Metadata {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
