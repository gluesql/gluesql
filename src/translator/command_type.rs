use crate::translator::Filter;
use nom_sql::{CreateTableStatement, InsertStatement};

pub enum CommandType {
    GetSchema(String),
    SetSchema(CreateTableStatement),
    SetData(InsertStatement),
    GetData(String, Filter),
    DelData(String, Filter),
}
