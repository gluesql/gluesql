mod sled_storage;

use sled::IVec;

use gluesql::store::Store;
use gluesql::Tester;

use sled_storage::SledStorage;

pub struct SledTester {
    storage: Box<SledStorage>,
}

impl SledTester {
    pub fn new(path: &str) -> Self {
        match std::fs::remove_dir_all(path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let storage = Box::new(SledStorage::new(path.to_owned()).expect("SledStorage::new"));

        SledTester { storage }
    }
}

impl Tester<IVec> for SledTester {
    fn get_storage(&mut self) -> &mut dyn Store<IVec> {
        &mut (*self.storage)
    }
}
