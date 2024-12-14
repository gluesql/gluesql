use {
    crate::{
        data::CustomFunction as StructCustomFunction,
        result::{Error, Result},
    },
    async_trait::async_trait,
};

#[cfg_attr(not(feature = "send"), async_trait(?Send))]
#[cfg_attr(feature = "send", async_trait)]
pub trait CustomFunction {
    async fn fetch_function(&self, _func_name: &str) -> Result<Option<&StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
    async fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
}

#[cfg_attr(not(feature = "send"), async_trait(?Send))]
#[cfg_attr(feature = "send", async_trait)]
pub trait CustomFunctionMut {
    async fn insert_function(&mut self, _func: StructCustomFunction) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }

    async fn delete_function(&mut self, _func_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
}
