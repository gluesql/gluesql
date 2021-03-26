use {crate::result::MutResult, async_trait::async_trait, std::ops::Range};

#[async_trait(?Send)]
pub trait AutoIncrement
where
    Self: Sized,
{
    async fn generate_values(
        self,
        table_name: &str,
        column_name: &str,
        size: usize,
    ) -> MutResult<Self, Range<i64>>;
}
