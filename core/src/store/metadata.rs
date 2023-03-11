use {
    crate::{prelude::Value, result::Result},
    async_trait::async_trait,
    std::{collections::HashMap, iter::empty},
};

type TableName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(TableName, HashMap<String, Value>)>>>;

#[async_trait(?Send)]
pub trait Metadata {
    async fn scan_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
