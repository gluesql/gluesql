use crate::translator::Row;
use nom_sql::CreateTableStatement;

pub trait Store {
    fn set_schema(&self, statement: CreateTableStatement) -> Result<(), ()>;

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement, &str>;

    fn set_data(&self, table_name: &str, row: Row) -> Result<(), ()>;

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = Row>>, ()>;

    fn del_data(&self, table_name: &str, key: &str) -> Result<(), ()>;
}
