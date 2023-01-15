#![cfg(feature = "function")]
use crate::{
    executor::FunctionProxy,
    result::{Error, Result},
};
use async_trait::async_trait;

#[async_trait(?Send)]
pub trait Function {
    fn register_function(&mut self, _name: &str, _proxy: FunctionProxy) -> Result<()> {
        let msg = "[Storage] Function::register_function is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
    async fn get_function(&self, _name: &str) -> Result<&FunctionProxy> {
        let msg = "[Storage] Function::get_function is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
    fn unregister_function(&mut self, _name: &str) -> Result<()> {
        let msg = "[Storage] Function::unregister_function is not supported".to_owned();

        Err(Error::StorageMsg(msg))
    }
}
