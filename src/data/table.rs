use {
    crate::{
        ast::{ObjectName, TableAlias, TableFactor},
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
}

impl<'a> Table<'a> {
    pub fn new(table_factor: &'a TableFactor) -> Result<Self> {
        match table_factor {
            TableFactor::Table { name, alias } => {
                let name = get_name(name)?;
                let alias = alias.as_ref().map(|TableAlias { name, .. }| name);

                Ok(Self { name, alias })
            }
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
}

pub fn get_name(table_name: &ObjectName) -> Result<&String> {
    let ObjectName(idents) = table_name;

    idents.last().ok_or_else(|| TableError::Unreachable.into())
}
