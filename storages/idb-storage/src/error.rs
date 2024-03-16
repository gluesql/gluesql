use {
    async_trait::async_trait,
    core::fmt::Display,
    gluesql_core::error::{Error, Result},
    std::{future::IntoFuture, result::Result as StdResult},
};

pub trait ErrInto<T> {
    fn err_into(self) -> Result<T>;
}

impl<T, E: Display> ErrInto<T> for Result<T, E> {
    fn err_into(self) -> Result<T> {
        self.map_err(|e| Error::StorageMsg(e.to_string()))
    }
}

#[async_trait(?Send)]
pub trait StoreReqIntoFuture<T> {
    async fn into_future(self) -> Result<T>;
}

#[async_trait(?Send)]
impl<F, T, E: Display> StoreReqIntoFuture<T> for Result<F, E>
where
    F: IntoFuture<Output = StdResult<T, E>>,
{
    async fn into_future(self) -> Result<T> {
        self.err_into()?.await.err_into()
    }
}
