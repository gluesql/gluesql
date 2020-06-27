use gluesql::{execute, Error, Payload, Result, Row, SledStorage, Store};

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use sled::IVec;
use std::fmt::Debug;

pub trait Helper<T: 'static + Debug> {
    fn get_storage(&self) -> &dyn Store<T>;

    fn run(&self, sql: &str) -> Result<Payload> {
        let dialect = GenericDialect {};
        let parsed = match Parser::parse_sql(&dialect, sql) {
            Ok(parsed) => parsed,
            Err(e) => {
                panic!("parse_query: {:?}", e);
            }
        };
        let parsed = &parsed[0];

        let storage = self.get_storage();

        println!("[Run] {}", parsed);
        execute(storage, parsed)
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

        assert_eq!(result.unwrap_err(), expected);
    }
}

pub struct SledHelper {
    storage: Box<SledStorage>,
}

impl SledHelper {
    pub fn new(path: &str) -> Self {
        match std::fs::remove_dir_all(path) {
            Ok(()) => (),
            Err(e) => {
                println!("fs::remove_file {:?}", e);
            }
        }

        let storage = Box::new(SledStorage::new(path.to_owned()).expect("SledStorage::new"));

        SledHelper { storage }
    }
}

impl Helper<IVec> for SledHelper {
    fn get_storage(&self) -> &dyn Store<IVec> {
        self.storage.as_ref()
    }
}

#[macro_export]
macro_rules! row {
    ( $( $p:path )* ; $( $v:expr )* ) => (
        Row(vec![$( $p($v) ),*])
    )
}

#[macro_export]
macro_rules! select {
    ( $( $t:path )* ; $( $v: expr )* ) => (
        Payload::Select(vec![
            row!($( $t )* ; $( $v )* )
        ])
    );
    ( $( $t:path )* ; $( $v: expr )* ; $( $( $v2: expr )* );*) => ({
        let mut rows = vec![
            row!($( $t )* ; $( $v )*),
        ];

        Payload::Select(
            concat!(rows ; $( $t )* ; $( $( $v2 )* );*)
        )
    });
}

#[macro_export]
macro_rules! concat {
    ( $rows:ident ; $( $t:path )* ; $( $v: expr )* ) => ({
        $rows.push(row!($( $t )* ; $( $v )*));

        $rows
    });
    ( $rows:ident ; $( $t:path )* ; $( $v: expr )* ; $( $( $v2: expr )* );* ) => ({
        $rows.push(row!($( $t )* ; $( $v )*));

        concat!($rows ; $( $t )* ; $( $( $v2 )* );* )
    });
}
