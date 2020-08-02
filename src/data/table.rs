use serde::Serialize;
use std::fmt::Debug;
use thiserror::Error;

use sqlparser::ast::{ObjectName, TableAlias, TableFactor};

use crate::result::Result;

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,

    #[error("TableFactorNotSupported")]
    TableFactorNotSupported,
}

pub struct Table<'a> {
    name: &'a String,
    alias: Option<&'a String>,
}

impl<'a> Table<'a> {
    pub fn new(table_factor: &'a TableFactor) -> Result<Self> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let name = get_name(name)?;
                let alias = alias.as_ref().map(|TableAlias { name, .. }| &name.value);

                Ok(Self { name, alias })
            }
            _ => Err(TableError::TableFactorNotSupported.into()),
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

pub fn get_name<'a>(table_name: &'a ObjectName) -> Result<&'a String> {
    let ObjectName(idents) = table_name;

    idents
        .last()
        .map(|ident| &ident.value)
        .ok_or_else(|| TableError::Unreachable.into())
}
