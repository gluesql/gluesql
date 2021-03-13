use {
    crate::{data::Value, result::MutResult},
    async_trait::async_trait,
};

#[async_trait(?Send)]
pub trait AutoIncrement
where
    Self: Sized,
{
    async fn generate_value(self, table_name: &str, column_name: &str) -> MutResult<Self, Value>;
}
