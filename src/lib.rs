pub mod data;
pub mod execute;
pub mod executor;
pub mod storage;

pub use data::*;
pub use execute::*;
pub use executor::*;
pub use storage::*;

pub mod error {
    use thiserror::Error;

    #[derive(Error, Debug)]
    pub enum Error {
        #[error(transparent)]
        Sled(#[from] sled::Error),
        #[error(transparent)]
        Bincode(#[from] bincode::Error),

        // storage
        #[error("not found")]
        NotFound,

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
}

pub use error::{Error, Result};
