use crate::result::Error;

use {
    crate::{
        ast::{IndexItem, ObjectName, TableAlias, TableFactor},
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
            TableFactor::Derived { alias, .. } => ObjectName(vec![alias.to_owned().name]),
            // TableFactor::Derived { subquery, alias } => {
            //     let alias = alias.as_ref().map(|TableAlias { name, .. }| name);
            //     select(subquery, None, false)
            //     select(storage, subquery, context)

            //     let name = alias.unwrap();
            //     Ok(Self {
            //         name,
            //         alias,
            //         index: None,
            //     })
            // }
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
    idents.last().ok_or_else(|| {
        print!(":+:+:+:ErrNo: 1");
        TableError::Unreachable.into()
    })
}
