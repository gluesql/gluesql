mod execute;
mod row;
mod storage;
mod translator;

use execute::{execute, Payload};
use nom_sql::parse_query;
use std::fmt::Debug;
use storage::{SledStorage, Store};

fn run<T: 'static + Debug>(storage: &dyn Store<T>, sql: &str) -> Result<Payload<T>, ()> {
    let parsed = parse_query(sql).unwrap();
    println!("[Run] {}", parsed);

    execute(storage, &parsed)
}

fn print<T: 'static + Debug>(result: Result<Payload<T>, ()>) {
    match result.unwrap() {
        Payload::Select(rows) => println!("[Ok ]\n{:#?}\n", rows),
        Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
        Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
        Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
        Payload::Create => println!("[Ok ] :)\n"),
    };
}

fn compare<T: 'static + Debug>(result: Result<Payload<T>, ()>, count: usize) {
    match result.unwrap() {
        Payload::Select(rows) => assert_eq!(rows.len(), count),
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

    for insert_sql in insert_sqls.iter() {
        run_sql(insert_sql).unwrap();
    }

    let test_cases = vec![
        (6, "SELECT * FROM TableA;"),
        (4, "SELECT * FROM TableA WHERE id = 3;"),
        (2, "SELECT * FROM TableA WHERE id = 3 AND test = 500;"),
        (6, "SELECT * FROM TableA WHERE id = 3 OR test = 100;"),
        (0, "SELECT * FROM TableA WHERE id != 3 AND test != 100;"),
        (2, "SELECT * FROM TableA WHERE id = 3 LIMIT 2;"),
        (4, "SELECT * FROM TableA LIMIT 10 OFFSET 2;"),
        (
            1,
            "SELECT * FROM TableA WHERE (id = 3 OR test = 100) AND test = 300;",
        ),
        (2, "SELECT * FROM TableA WHERE NOT (id = 3);"),
        (2, "UPDATE TableA SET test = 200 WHERE test = 100;"),
        (0, "SELECT * FROM TableA WHERE test = 100;"),
        (2, "SELECT * FROM TableA WHERE (test = 200);"),
        (2, "DELETE FROM TableA WHERE id != 3;"),
        (4, "SELECT * FROM TableA;"),
        (4, "DELETE FROM TableA;"),
    ];

    for (num, sql) in test_cases {
        compare(run_sql(sql), num);
    }

    for insert_sql in insert_sqls.iter() {
        run_sql(insert_sql).unwrap();
    }

    let test_select = |sql, num| {
        match run_sql(sql).unwrap() {
            Payload::Select(rows) => assert_eq!(rows.into_iter().nth(0).unwrap().items.len(), num),
            _ => assert!(false),
        };
    };

    let select_sql = "SELECT id, test FROM TableA;";
    test_select(select_sql, 2);

    let select_sql = "SELECT id FROM TableA;";
    test_select(select_sql, 1);

    let select_sql = "SELECT * FROM TableA;";
    test_select(select_sql, 2);

    println!("\n\n");
}
