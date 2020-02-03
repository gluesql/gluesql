use nom_sql::{CreateTableStatement, SelectStatement, DeleteStatement, InsertStatement, UpdateStatement};

pub enum CommandType<'a> {
    Create(&'a CreateTableStatement),
    Insert(&'a InsertStatement),
    Select(&'a SelectStatement),
    Delete(&'a DeleteStatement),
    Update(&'a UpdateStatement),
}
