use crate::data::CustomFunction;
use crate::result::{Error, Result};
use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Function {
    async fn fetch_function(&self, _func_name: &str) -> Result<Option<&CustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] Function is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
pub trait FunctionMut {
    async fn create_function(&mut self, _func: CustomFunction) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] Function is not supported".to_owned(),
        ))
    }

    async fn drop_function(&mut self, _func_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] Function is not supported".to_owned(),
        ))
    }
}
