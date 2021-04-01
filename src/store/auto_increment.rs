use {crate::result::MutResult, async_trait::async_trait};

#[async_trait(?Send)]
pub trait AutoIncrement
where
    Self: Sized,
{
    async fn generate_increment_values(
        self,
        table_name: String,
        columns: Vec<(
            usize,  /*index*/
            String, /*name*/
            i64,    /*row_count*/
        ) /*column*/>,
    ) -> MutResult<
        Self,
        Vec<(
            (usize /*index*/, String /*name*/), /*column*/
            i64,                                /*start_value*/
        )>,
    >;

    async fn set_increment_value(
        self,
        table_name: &str,
        column_name: &str,
        end: i64,
    ) -> MutResult<Self, ()>;
}
