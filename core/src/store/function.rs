use {
    crate::{
        data::CustomFunction as StructCustomFunction,
        result::{Error, Result},
    },
    async_trait::async_trait,
};

#[async_trait(?Send)]
pub trait CustomFunction {
    async fn fetch_function<'a>(
        &'a self,
        _func_name: &str,
    ) -> Result<Option<&'a StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }

    async fn fetch_all_functions<'a>(&'a self) -> Result<Vec<&'a StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
}

#[async_trait(?Send)]
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
