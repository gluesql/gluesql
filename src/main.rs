mod parser;
mod translator;
mod executor;
mod storage;

use parser::parse;
use translator::translate;
use executor::execute;
use storage::SledStorage;

fn main() {
    println!("\n\n");
    println!("Hello, world!");

    let raw_sql = String::from("SELECT * FROM TableA");
    let query_node = parse(raw_sql);
    let command_queue = translate(query_node);
    let storage = SledStorage::new(String::from("data.db"));

    execute(&storage, command_queue);

    println!("\n\n");
}
