#[cfg(feature = "sled-storage")]
use crate::{execute, storages::SledStorage, Payload, Query, Result};
#[cfg(feature = "sled-storage")]
use futures::executor::block_on;

#[cfg(feature = "sled-storage")]
pub struct Glue {
    storage: Option<SledStorage>,
}

#[cfg(feature = "sled-storage")]
impl Glue {
    pub fn new(storage: SledStorage) -> Self {
        let storage = Some(storage);

        Self { storage }
    }

    pub fn execute(&mut self, query: &Query) -> Result<Payload> {
        let storage = self.storage.take().unwrap();

        match block_on(execute(storage, query)) {
            Ok((storage, payload)) => {
                self.storage = Some(storage);

                Ok(payload)
            }
            Err((storage, error)) => {
                self.storage = Some(storage);

                Err(error)
            }
        }
    }
}
