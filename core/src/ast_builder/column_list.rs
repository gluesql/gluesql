use {
    crate::{
        parse_sql::parse_identifiers,
        result::{Error, Result},
        translate::translate_idents,
    },
};


#[derive(Clone)]
pub enum ColumnList {
    Text(String),
    Columns(Vec<String>),
}

impl From<&str> for ColumnList {
    fn from(columns: &str) -> Self {
        ColumnList::Text(columns.to_owned())
    }
}

impl From<Vec<&str>> for ColumnList {
    fn from(columns: Vec<&str>) -> Self {
        ColumnList::Columns(columns.into_iter().map(ToOwned::to_owned).collect())
    }
}

impl TryFrom<ColumnList> for Vec<String> {
    type Error = Error;

    fn try_from(column_list: ColumnList) -> Result<Self> {
        match column_list {
            ColumnList::Text(columns) => {
                let idents = parse_identifiers(columns)?;
                Ok(translate_idents(idents.as_slice()))
            }
            ColumnList::Columns(columns) => Ok(columns),
        }
    }
}

