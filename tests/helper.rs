use gluesql::{bail, execute, Error, Payload, Result, Row, SledStorage, Store};
use nom_sql::parse_query;
use sled::IVec;
use std::fmt::Debug;

pub trait Helper<T: 'static + Debug> {
    fn get_storage(&self) -> &dyn Store<T>;

    fn run(&self, sql: &str) -> Result<Payload> {
        let parsed = match parse_query(sql) {
            Ok(parsed) => parsed,
            Err(e) => bail!("failed to parse query: {:?}", e),
        };

        let storage = self.get_storage();

        println!("[Run] {}", parsed);
        execute(storage, &parsed)
    }

    fn run_and_print(&self, sql: &str) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => println!("[Ok ]\n{:#?}\n", rows),
            Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
            Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
            Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
            Payload::Create => println!("[Ok ] :)\n"),
        };
    }

    fn test_rows(&self, sql: &str, count: usize) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => assert_eq!(count, rows.len()),
            Payload::Delete(num) => assert_eq!(count, num),
            Payload::Update(num) => assert_eq!(count, num),
            _ => panic!("compare is only for Select, Delete and Update"),
        };
    }

    fn test_columns(&self, sql: &str, count: usize) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => {
                let Row(items) = rows.into_iter().next().unwrap();

                assert_eq!(count, items.len())
            }
            _ => assert!(false),
        };
    }

    fn test_error(&self, sql: &str, expected: Error) {
        let result = self.run(sql);

        match (result.unwrap_err(), expected) {
            (Error::Execute(found), Error::Execute(expected)) => assert_eq!(expected, found),
            (Error::Select(found), Error::Select(expected)) => assert_eq!(expected, found),
            (Error::Join(found), Error::Join(expected)) => assert_eq!(expected, found),
            (Error::Blend(found), Error::Blend(expected)) => assert_eq!(expected, found),
            (found, expected) => panic!(
                "\n\n    test: {}\nexpected: {:?}\n   found: {:?}\n\n",
                sql, expected, found
            ),
        }
    }
}

pub struct SledHelper {
    storage: Box<SledStorage>,
}

impl SledHelper {
    pub fn new(path: &str) -> Self {
        std::fs::remove_dir_all(path).expect("fs::remove_file");
        let storage = Box::new(SledStorage::new(path.to_string()).expect("SledStorage::new"));

        SledHelper { storage }
    }
}

impl Helper<IVec> for SledHelper {
    fn get_storage(&self) -> &dyn Store<IVec> {
        self.storage.as_ref()
    }
}
