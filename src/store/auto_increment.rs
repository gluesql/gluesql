use {
    crate::{result::MutResult, Row},
    async_trait::async_trait,
    sqlparser::ast::ColumnDef,
};

#[async_trait(?Send)]
pub trait AutoIncrement
where
    Self: Sized,
{
    async fn generate_values(
        self,
        table_name: &str,
        columns: Vec<(usize, &ColumnDef)>,
        rows: Vec<Row>,
    ) -> MutResult<Self, Vec<Row>>;
}
