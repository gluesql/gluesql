mod command;
mod helper;
mod print;

use {
    crate::{
        command::Command,
        helper::CliHelper,
        print::{print_help, print_payload},
    },
    gluesql::prelude::*,
    rustyline::{error::ReadlineError, Editor},
};

fn main() {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    let temp_queries = [
        "CREATE TABLE sample (id INTEGER, name TEXT, date DATE)",
        r#"INSERT INTO sample VALUES
            (1, "Foo", "2020-01-01"),
            (2, "Bar", "1989-04-11"),
            (3, "Hello", "1991-12-01"),
            (4, "World", "2001-04-15"),
            (5, "Greet", "2101-06-11"),
            (9, "Ginger", "2050-03-01"),
            (10, "Paint", "2000-01-11");
        "#,
        "CREATE TABLE test (id INTEGER);",
        "INSERT INTO test VALUES (1), (10), (100), (200), (404);",
    ];

    for query in temp_queries {
        glue.execute(query).expect("Execution failed");
    }

    print_help();

    let mut rl = Editor::<CliHelper>::new();
    rl.set_helper(Some(CliHelper::default()));

    loop {
        let line = match rl.readline("gluesql> ") {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("bye\n");
                break;
            }
            Err(e) => {
                println!("[unknown error] {:?}", e);
                break;
            }
        };

        rl.add_history_entry(line.as_str());

        let command = match Command::parse(line.as_str()) {
            Ok(command) => command,
            Err(_) => {
                println!("[error] unsupported command: {}", line);
                continue;
            }
        };

        match command {
            Command::Help => {
                print_help();
                continue;
            }
            Command::Quit => {
                println!("bye\n");
                break;
            }
            Command::Execute(sql) => match glue.execute(sql.as_str()) {
                Ok(payload) => print_payload(payload),
                Err(e) => println!("[error] {}\n", e),
            },
        }
    }
}
