use std::fs;

use gluesql_core::ast::Statement::CreateTable;
use gluesql_core::prelude::{parse, translate};

use crate::JsonlStorage;

#[test]
fn read_schema() {
    let path = "data/sample.sql";
    let file = fs::read_to_string(path).unwrap();
    let parsed = parse(&file).unwrap();
    let translated = translate(&parsed[0]).unwrap();
    match translated {
        CreateTable { name, columns, .. } => {
            let jsonl_storage = JsonlStorage::default();
            println!("{name}");
            println!("{columns:#?}");
            jsonl_storage.load_table(name, columns);
        }
        _ => unreachable!(),
    }
}
