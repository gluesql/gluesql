use crate::ExecuteError;
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    // storage
    #[error("not found")]
    NotFound,

    #[error(transparent)]
    Execute(#[from] ExecuteError),

    // all other errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::err!($($arg)*));
    };
}

#[macro_export]
macro_rules! err {
    ($($arg:tt)*) => {
        Error::Other(anyhow::anyhow!($($arg)*))
    };
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $($arg:tt)*) => {
        if !$cond {
            $crate::bail!($($arg)*);
        }
    };
}
