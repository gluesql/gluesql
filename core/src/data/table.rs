use {
    crate::{
        ast::{IndexItem, ObjectName, TableAlias, TableFactor},
        result::Error,
        result::Result,
    },
    serde::Serialize,
    std::fmt::Debug,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,
}

pub struct Table<'a> {
    name: &'a String,
    alias: Option<&'a String>,
    index: Option<&'a IndexItem>,
}

impl<'a> Table<'a> {
    pub fn new(table_factor: &'a TableFactor) -> Result<Self> {
        match table_factor {
            TableFactor::Table { name, alias, index } => {
                let name = get_name(name)?;
                let alias = alias.as_ref().map(|TableAlias { name, .. }| name);
                let index = index.as_ref();

                Ok(Self { name, alias, index })
            }
            TableFactor::Derived { .. } => Err(Error::Table(TableError::Unreachable)),
        }
    }

    pub fn get_name(&self) -> &'a String {
        self.name
    }

    pub fn get_alias(&self) -> &'a String {
        match self.alias {
            Some(alias) => alias,
            None => self.name,
        }
    }

    pub fn get_index(&self) -> Option<&'a IndexItem> {
        self.index
    }
}

pub fn get_name(table_name: &ObjectName) -> Result<&String> {
    let ObjectName(idents) = table_name;
    idents.last().ok_or_else(|| TableError::Unreachable.into())
}
