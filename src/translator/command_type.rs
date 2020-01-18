use crate::translator::{Filter, Update};
use nom_sql::{CreateTableStatement, InsertStatement};

pub enum CommandType {
    Create(CreateTableStatement),
    Insert(InsertStatement),
    Select(String, Filter),
    Delete(String, Filter),
    Update(String, Update, Filter),
}
