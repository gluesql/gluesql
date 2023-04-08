use {
    core::fmt::Display,
    gluesql_core::result::{Error, Result},
};

pub trait ErrInto<T> {
    fn err_into(self) -> Result<T>;
}

impl<T, E: Display> ErrInto<T> for Result<T, E> {
    fn err_into(self) -> Result<T> {
        self.map_err(|e| Error::StorageMsg(e.to_string()))
    }
}
