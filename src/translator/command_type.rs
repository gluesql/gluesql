use crate::translator::{Blend, Filter, Limit, Update};
use nom_sql::{CreateTableStatement, InsertStatement};

pub struct SelectTranslation<'a> {
    pub table_name: &'a str,
    pub blend: Blend<'a>,
    pub filter: Filter<'a>,
    pub limit: Limit<'a>,
}

pub enum CommandType<'a> {
    Create(&'a CreateTableStatement),
    Insert(&'a InsertStatement),
    Select(SelectTranslation<'a>),
    Delete {
        table_name: &'a str,
        filter: Filter<'a>,
    },
    Update {
        table_name: &'a str,
        update: Update<'a>,
        filter: Filter<'a>,
    },
}
