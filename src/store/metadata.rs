use {crate::result::Result, async_trait::async_trait};

#[async_trait(?Send)]
pub trait Metadata {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }

    async fn schema_names(&self) -> Result<Vec<String>>;
}
