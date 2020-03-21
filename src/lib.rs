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
    }

    pub type Result<T> = std::result::Result<T, Error>;
}
