use crate::translator::Filter;
use nom_sql::{CreateTableStatement, InsertStatement};

pub enum CommandType {
    SetSchema(CreateTableStatement),
    SetData(InsertStatement),
    GetData(String, Filter),
    DelData(String, Filter),
}
