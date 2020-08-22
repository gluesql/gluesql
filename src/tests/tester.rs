use crate::data::Row;
use crate::executor::Payload;
use crate::parse::{parse, Query};
use crate::result::{Error, Result};

/// If you want to make your custom storage and want to run integrate tests,
/// you should implement this `Tester` trait.
///
/// To see how to use it,
/// * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/tests/sled_storage.rs)
///
/// Actual test cases are in [/src/tests/](https://github.com/gluesql/gluesql/blob/main/src/tests/),
/// not in `/tests/`.
pub trait Tester {
    fn new(namespace: &str) -> Self;

    fn execute(&mut self, query: &Query) -> Result<Payload>;

    fn run(&mut self, sql: &str) -> Result<Payload> {
        println!("[Run] {}", sql);

        parse(sql)
            .unwrap()
            .iter()
            .map(|query| self.execute(query))
            .next()
            .unwrap()
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
