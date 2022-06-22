use crate::{
    ast::{Join, Query},
    result::Error,
    store::GStore,
};

use {
    crate::{
        ast::{IndexItem, ObjectName, Select, SelectItem, SetExpr, TableAlias, TableFactor},
        executor::get_labels,
        result::Result,
    },
    futures::stream,
    serde::Serialize,
    thiserror::Error,
};

#[derive(Error, Serialize, Debug, PartialEq)]
pub enum TableError {
    #[error("unreachable")]
    Unreachable,
}

pub enum RelationType {
    Table,
    Derived,
}

pub struct Relation<'a> {
    name: &'a String,
    alias: Option<&'a String>,
    index: Option<&'a IndexItem>,
    relation_type: RelationType,
}

impl<'a> Relation<'a> {
    pub fn new(table_factor: &'a TableFactor) -> Result<Self> {
        match table_factor {
            TableFactor::Table { name, alias, index } => {
                let name = get_name(name)?;
                let alias = alias.as_ref().map(|TableAlias { name, .. }| name);
                let index = index.as_ref();
                let relation_type = RelationType::Table;
                Ok(Self {
                    name,
                    alias,
                    index,
                    relation_type,
                })
            }
            TableFactor::Derived { alias, .. } => {
                let name = &alias.name;
                let alias = Some(name);
                let index = None;
                let relation_type = RelationType::Derived;
                Ok(Self {
                    name,
                    alias,
                    index,
                    relation_type,
                })
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

    pub fn get_index(&self) -> Option<&'a IndexItem> {
        self.index
    }

    // pub fn get_labels<'b>(
    //     &self,
    //     projection: &[SelectItem],
    //     // table_alias: &str,
    //     // table_factor: &'a TableFactor,
    //     columns: &'b [String],
    //     join_columns: Option<&'b [(&String, Vec<String>)]>,
    // ) -> Result<Vec<String>> {
    //     // #[derive(Iterator)]
    //     // enum Labeled<I1, I2, I3, I4> {
    //     //     Err(I1),
    //     //     Wildcard(I2),
    //     //     QualifiedWildcard(I3),
    //     //     Once(I4),
    //     // }

    //     // let err = |e| Labeled::Err(once(Err(e)));
    //     match self.relation_type {
    //         RelationType::Table => get_labels(projection, self.get_alias(), &columns, join_columns),
    //         RelationType::Derived => get_labels(projection, self.get_alias(), &columns, None),
    //     }
    // }
}

pub fn get_name(table_name: &ObjectName) -> Result<&String> {
    let ObjectName(idents) = table_name;
    idents.last().ok_or_else(|| TableError::Unreachable.into())
}
