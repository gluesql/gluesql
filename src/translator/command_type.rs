use nom_sql::CreateTableStatement;

pub enum CommandType {
    GetSchema(String),
    SetSchema(CreateTableStatement),
}
