mod executor;
mod storage;
mod translator;

use executor::{execute, Payload};
use nom_sql::parse_query;
use storage::SledStorage;
use translator::{translate, Row};

fn run(storage: &SledStorage, sql: String) {
    let parsed = parse_query(&sql).unwrap();
    println!("[Run] {}", parsed);

    let command_queue = translate(parsed);

    match execute(storage, command_queue).unwrap() {
        Payload::Select(rows) => {
            let rows = rows.collect::<Vec<Row<u64>>>();

            println!("[Ok ]\n{:#?}\n", rows);
        }
        Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
        Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
        Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
        Payload::Create => println!("[Ok ] :)\n"),
    };
}

fn main() {
    println!("\n\n");

    let storage = SledStorage::new(String::from("data.db"));
    let run_sql = |sql| run(&storage, sql);

    let create_sql = String::from(
        "
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
        );
    ",
    );
    run_sql(create_sql);

    let insert_sql = String::from("INSERT INTO TableA (id, test) VALUES (1, 100);");
    run_sql(insert_sql);

    let insert_sql = String::from("INSERT INTO TableA (id, test) VALUES (2, 100);");
    run_sql(insert_sql);

    let select_sql = String::from("SELECT * FROM TableA;");
    run_sql(select_sql);

    let update_sql = String::from("UPDATE TableA SET test = 200 WHERE test = 100;");
    run_sql(update_sql);

    let select_sql = String::from("SELECT * FROM TableA WHERE test = 200;");
    run_sql(select_sql);

    let delete_sql = String::from("DELETE FROM TableA WHERE test = 200;");
    run_sql(delete_sql);

    println!("\n\n");
}
