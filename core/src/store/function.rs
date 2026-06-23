use crate::{
    data::CustomFunction as StructCustomFunction,
    result::{Error, Result},
};

pub trait CustomFunction {
    fn fetch_function<'a>(&'a self, _func_name: &str) -> Result<Option<&'a StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }

    fn fetch_all_functions(&self) -> Result<Vec<&StructCustomFunction>> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
}

pub trait CustomFunctionMut {
    fn insert_function(&mut self, _func: StructCustomFunction) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }

    fn delete_function(&mut self, _func_name: &str) -> Result<()> {
        Err(Error::StorageMsg(
            "[Storage] CustomFunction is not supported".to_owned(),
        ))
    }
}
