use sled::Db;
use sled::IVec;

use crate::storage::Store;

pub struct SledStorage {
    tree: Db,
}

impl SledStorage {
    pub fn new(filename: String) -> SledStorage {
        let tree = Db::open(filename).unwrap();

        SledStorage { tree }
    }
}

impl Store for SledStorage {
    fn get(&self) {
        println!("SledStorage get!");

        let k = b"asdf";
        let v1 = IVec::from(b"1928");

        let tree = &self.tree;

        assert_eq!(tree.get(&k), Ok(Some(v1)));
        assert_eq!(tree.get(&k).unwrap().unwrap(), b"1928");
    }

    fn set(&self) {
        println!("SledStorage set!");

        let k = b"asdf";
        let v1 = b"1928";

        let tree = &self.tree;

        match tree.insert(k, v1) {
            Ok(_) => { println!("insert succeeded"); },
            Err(_) => { println!("insert failed"); },
        }
    }
}
