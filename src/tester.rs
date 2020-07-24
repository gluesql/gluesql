use super::data::Row;
use super::executor::Payload;
use super::result::{Error, Result};

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct TestQuery<'a>(pub &'a Statement);

pub trait Tester {
    fn execute(&mut self, parsed: TestQuery) -> Result<Payload>;

    fn run(&mut self, sql: &str) -> Result<Payload> {
        let dialect = GenericDialect {};
        let parsed = match Parser::parse_sql(&dialect, sql) {
            Ok(parsed) => parsed,
            Err(e) => {
                panic!("parse_query: {:?}", e);
            }
        };
        let parsed = &parsed[0];

        println!("[Run] {}", parsed);
        self.execute(TestQuery(parsed))
    }

    fn run_and_print(&mut self, sql: &str) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => println!("[Ok ]\n{:#?}\n", rows),
            Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
            Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
            Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
            Payload::DropTable => println!("[Ok ] :)\n"),
            Payload::Create => println!("[Ok ] :)\n"),
        };
    }

    fn test_rows(&mut self, sql: &str, count: usize) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => assert_eq!(count, rows.len()),
            Payload::Delete(num) => assert_eq!(count, num),
            Payload::Update(num) => assert_eq!(count, num),
            _ => panic!("compare is only for Select, Delete and Update"),
        };
    }

    fn test_columns(&mut self, sql: &str, count: usize) {
        let result = self.run(sql);

        match result.unwrap() {
            Payload::Select(rows) => {
                let Row(items) = rows.into_iter().next().unwrap();

                assert_eq!(count, items.len())
            }
            _ => panic!("tests_columns can only handle SELECT"),
        };
    }

    fn test_error(&mut self, sql: &str, expected: Error) {
        let result = self.run(sql);

        assert_eq!(result.unwrap_err(), expected);
    }
}
