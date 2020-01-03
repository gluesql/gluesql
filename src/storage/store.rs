use nom_sql::CreateTableStatement;

pub trait Store {
    fn set_schema(&self, _statement: CreateTableStatement) -> Result<(), ()>;

    fn get_schema(&self, _table_name: String) -> Result<CreateTableStatement, &str>;
}
