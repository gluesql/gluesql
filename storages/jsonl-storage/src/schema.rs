use std::fs::{self, File};
use std::io::Read;

use futures::executor::block_on;
use gluesql_core::ast::Statement::CreateTable;
use gluesql_core::chrono::NaiveDateTime;
use gluesql_core::data::Schema;
use gluesql_core::prelude::{parse, translate, Glue};
use gluesql_core::store::Store;

use crate::JsonlStorage;

// #[test]
// fn read_schema() {
//     let path = "data/sample.sql";
//     let file = fs::read_to_string(path).unwrap();
//     let parsed = parse(&file).unwrap();
//     let translated = translate(&parsed[0]).unwrap();
//     match translated {
//         CreateTable { name, columns, .. } => {
//             let mut jsonl_storage = JsonlStorage::default();
//             println!("{name}");
//             println!("{columns:#?}");

//             let schema = Schema {
//                 table_name: name.clone(),
//                 column_defs: Some(columns),
//                 indexes: Vec::new(),
//                 created: NaiveDateTime::default(), // todo!: parse comment
//             };
//             jsonl_storage.insert_schema(&schema);

//             block_on(async {
//                 let actual = jsonl_storage.fetch_schema(&name).await;
//                 let expected = Ok(Some(schema));

//                 assert_eq!(actual, expected)
//             })
//         }
//         _ => unreachable!(),
//     }
// }

// #[test]
// fn write_schema_file() {
//     let statement: String = "CREATE TABLE Items (id INT NULL, name TEXT NULL);".to_string();
//     let path = "data";
//     let jsonl_storage = JsonlStorage::new(path).unwrap();
//     let mut glue = Glue::new(jsonl_storage);
//     glue.execute(statement.clone()).unwrap();
//     let mut s = String::new();
//     File::open(format!("{path}/Items.sql"))
//         .unwrap()
//         .read_to_string(&mut s)
//         .unwrap();
//     assert_eq!(s, statement);

//     glue.execute("INSERT INTO Items VALUES(1, 'a')").unwrap();
// }
