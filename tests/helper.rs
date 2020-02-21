use gluesql::{execute, Payload, Store};
use nom_sql::parse_query;
use std::fmt::Debug;

pub fn run<T: 'static + Debug>(storage: &dyn Store<T>, sql: &str) -> Result<Payload<T>, ()> {
    let parsed = parse_query(sql).unwrap();
    println!("[Run] {}", parsed);

    execute(storage, &parsed)
}

pub fn print<T: 'static + Debug>(result: Result<Payload<T>, ()>) {
    match result.unwrap() {
        Payload::Select(rows) => println!("[Ok ]\n{:#?}\n", rows),
        Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
        Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
        Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
        Payload::Create => println!("[Ok ] :)\n"),
    };
}

pub fn compare<T: 'static + Debug>(result: Result<Payload<T>, ()>, count: usize) {
    match result.unwrap() {
        Payload::Select(rows) => assert_eq!(rows.len(), count),
        Payload::Delete(num) => assert_eq!(num, count),
        Payload::Update(num) => assert_eq!(num, count),
        _ => panic!("compare is only for Select, Delete and Update"),
    };
}
