use {
    crate::result::{MutResult, Result},
    async_trait::async_trait,
    std::ops::Range,
};

#[async_trait(?Send)]
pub trait AutoIncrement
where
    Self: Sized,
{
    async fn get_increment_value(&self, table_name: &str, column_name: &str) -> Result<i64>;

    async fn set_increment_value(
        self,
        table_name: &str,
        column_name: &str,
        end: i64,
    ) -> MutResult<Self, ()>;
}
