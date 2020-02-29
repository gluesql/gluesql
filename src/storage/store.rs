use crate::data::Row;
use nom_sql::CreateTableStatement;

pub trait Store<T: std::fmt::Debug> {
    fn gen_id(&self) -> Result<T, ()>;

    fn set_schema(&self, statement: &CreateTableStatement) -> Result<(), ()>;

    fn get_schema(&self, table_name: &str) -> Result<CreateTableStatement, &str>;

    fn set_data(&self, table_name: &str, row: Row<T>) -> Result<Row<T>, ()>;

    fn get_data(&self, table_name: &str) -> Result<Box<dyn Iterator<Item = Row<T>>>, ()>;

    fn del_data(&self, table_name: &str, key: &T) -> Result<(), ()>;
}
