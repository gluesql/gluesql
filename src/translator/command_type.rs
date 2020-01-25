use crate::translator::{Blend, Filter, Limit, Update};
use nom_sql::{CreateTableStatement, InsertStatement};

pub enum CommandType {
    Create(CreateTableStatement),
    Insert(InsertStatement),
    Select {
        table_name: String,
        blend: Blend,
        filter: Filter,
        limit: Limit,
    },
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
