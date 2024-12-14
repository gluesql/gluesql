use {
    crate::{prelude::Value, result::Result},
    async_trait::async_trait,
    std::{collections::HashMap, iter::empty},
};

type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, HashMap<String, Value>)>>>;

#[cfg_attr(not(feature = "send"), async_trait(?Send))]
#[cfg_attr(feature = "send", async_trait)]
pub trait Metadata {
    async fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
