use crate::translator::{Blend, Filter, Limit, Update};
use nom_sql::{CreateTableStatement, InsertStatement};

pub struct SelectTranslation {
    pub table_name: String,
    pub blend: Blend,
    pub filter: Filter,
    pub limit: Limit,
}

pub enum CommandType {
    Create(CreateTableStatement),
    Insert(InsertStatement),
    Select(SelectTranslation),
    Delete {
        table_name: String,
        filter: Filter,
    },
    Update {
        table_name: String,
        update: Update,
        filter: Filter,
    },
}
