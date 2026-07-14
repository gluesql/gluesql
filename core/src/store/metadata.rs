use {
    crate::{prelude::Value, result::Result},
    std::{collections::BTreeMap, iter::empty},
};

type ObjectName = String;
pub type MetaIter = Box<dyn Iterator<Item = Result<(ObjectName, BTreeMap<String, Value>)>>>;

pub trait Metadata {
    fn scan_table_meta(&self) -> Result<MetaIter> {
        Ok(Box::new(empty()))
    }
}
