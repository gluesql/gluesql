#![cfg(feature = "function")]

use {
    crate::MemoryStorage,
    async_trait::async_trait,
    gluesql_core::{
        prelude::FunctionProxy,
        result::{Error, Result},
        store::Function,
    },
};

#[async_trait(?Send)]
impl Function for MemoryStorage {
    fn register_function(&mut self, _name: &str, _proxy: FunctionProxy) -> Result<()> {
        self.functions.insert(_name.to_uppercase(), _proxy);
        Ok(())
    }
    async fn get_function(&self, _name: &str) -> Result<&FunctionProxy> {
        if let Some(f) = self.functions.get(_name) {
            Ok(f)
        } else {
            Err(Error::StorageMsg(format!(
                "[MemoryStorage] Function {_name} not registered"
            )))
        }
    }
}
