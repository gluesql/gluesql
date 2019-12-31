mod translator;
mod executor;
mod storage;

use nom_sql::parse_query;
use translator::translate;
use executor::execute;
use storage::SledStorage;

fn main() {
    println!("\n\n");

    let raw_sql = String::from("
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
        );
    ");

    let parsed = parse_query(&raw_sql).unwrap();

    println!("{:#?}", parsed);

    let command_queue = translate(parsed);
    let storage = SledStorage::new(String::from("data.db"));

    execute(&storage, command_queue);

    println!("\n\n");
}
