use crate::translator::{Filter, Limit, Update};
use nom_sql::{CreateTableStatement, InsertStatement};

pub enum CommandType {
    Create(CreateTableStatement),
    Insert(InsertStatement),
    Select(String, Filter, Limit),
    Delete(String, Filter),
    Update(String, Update, Filter),
}
