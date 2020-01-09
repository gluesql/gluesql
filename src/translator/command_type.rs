use nom_sql::{CreateTableStatement, InsertStatement, SelectStatement};

pub enum CommandType {
    GetSchema(String),
    SetSchema(CreateTableStatement),
    SetData(InsertStatement),
    GetData(SelectStatement),
}
