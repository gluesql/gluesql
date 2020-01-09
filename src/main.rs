mod executor;
mod storage;
mod translator;

use executor::execute;
use nom_sql::parse_query;
use storage::SledStorage;
use translator::translate;

fn run_sql(sql: String) {
    let parsed = parse_query(&sql).unwrap();
    let command_queue = translate(parsed);
    let storage = SledStorage::new(String::from("data.db"));

    execute(&storage, command_queue).unwrap();
}

fn main() {
    println!("\n\n");

    let create_sql = String::from(
        "
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
        );
    ",
    );
    run_sql(create_sql);

    let insert_sql = String::from("INSERT INTO TableA (id, test) VALUES (191, 12938);");
    run_sql(insert_sql);

    let select_sql = String::from("SELECT * FROM TableA;");
    run_sql(select_sql);

    println!("\n\n");
}
