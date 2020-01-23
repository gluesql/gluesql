mod executor;
mod storage;
mod translator;

use executor::{execute, Payload};
use nom_sql::parse_query;
use std::fmt::Debug;
use storage::{SledStorage, Store};
use translator::{translate, Row};

fn run<T: 'static + Debug>(storage: &dyn Store<T>, sql: &str) -> Result<Payload<T>, ()> {
    let parsed = parse_query(sql).unwrap();
    println!("[Run] {}", parsed);

    let command_queue = translate(parsed);

    execute(storage, command_queue)
}

fn print<T: 'static + Debug>(result: Result<Payload<T>, ()>) {
    match result.unwrap() {
        Payload::Select(rows) => {
            let rows = rows.collect::<Vec<Row<T>>>();

            println!("[Ok ]\n{:#?}\n", rows);
        }
        Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
        Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
        Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
        Payload::Create => println!("[Ok ] :)\n"),
    };
}

fn compare<T: 'static + Debug>(result: Result<Payload<T>, ()>, count: usize) {
    match result.unwrap() {
        Payload::Select(rows) => assert_eq!(rows.count(), count),
        Payload::Delete(num) => assert_eq!(num, count),
        Payload::Update(num) => assert_eq!(num, count),
        _ => panic!("compare is only for Select, Delete and Update"),
    };
}

fn main() {
    println!("\n\n");

    let storage = SledStorage::new(String::from("data.db"));
    let run_sql = |sql| run(&storage, sql);

    let create_sql = "
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
        );
    ";

    print(run_sql(create_sql));

    let delete_sql = "DELETE FROM TableA";
    print(run_sql(delete_sql));

    let insert_sqls: [&str; 6] = [
        "INSERT INTO TableA (id, test) VALUES (1, 100);",
        "INSERT INTO TableA (id, test) VALUES (2, 100);",
        "INSERT INTO TableA (id, test) VALUES (3, 300);",
        "INSERT INTO TableA (id, test) VALUES (3, 400);",
        "INSERT INTO TableA (id, test) VALUES (3, 500);",
        "INSERT INTO TableA (id, test) VALUES (3, 500);",
    ];

    for insert_sql in insert_sqls.into_iter() {
        run_sql(insert_sql).unwrap();
    }

    let select_sql = "SELECT * FROM TableA;";
    compare(run_sql(select_sql), 6);

    let select_sql = "SELECT * FROM TableA WHERE id = 3;";
    compare(run_sql(select_sql), 4);

    let select_sql = "SELECT * FROM TableA WHERE id = 3 AND test = 500;";
    compare(run_sql(select_sql), 2);

    let select_sql = "SELECT * FROM TableA WHERE id = 3 OR test = 100;";
    compare(run_sql(select_sql), 6);

    let select_sql = "SELECT * FROM TableA WHERE id != 3 AND test != 100;";
    compare(run_sql(select_sql), 0);

    let select_sql = "SELECT * FROM TableA WHERE id = 3 LIMIT 2;";
    compare(run_sql(select_sql), 2);

    let select_sql = "SELECT * FROM TableA LIMIT 10 OFFSET 2;";
    compare(run_sql(select_sql), 4);

    let update_sql = "UPDATE TableA SET test = 200 WHERE test = 100;";
    compare(run_sql(update_sql), 2);

    let select_sql = "SELECT * FROM TableA WHERE test = 100;";
    compare(run_sql(select_sql), 0);

    let select_sql = "SELECT * FROM TableA WHERE test = 200;";
    compare(run_sql(select_sql), 2);

    let delete_sql = "DELETE FROM TableA;";
    compare(run_sql(delete_sql), 6);

    println!("\n\n");
}
