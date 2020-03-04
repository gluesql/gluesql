use crate::data::Row;
use nom_sql::CreateTableStatement;

pub trait Store<T: std::fmt::Debug> {
    fn gen_id(&self, table_name: &str) -> Result<T, ()>;

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<(), ()>;

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement, &str>;

    fn set_data(&self, key: &T, row: Row) -> Result<Row, ()>;

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = (T, Row)>>, ()>;

    fn del_data(&self, key: &T) -> Result<(), ()>;
}
