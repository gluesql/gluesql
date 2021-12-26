mod cli;
mod command;
mod helper;
mod print;

use {crate::cli::Cli, gluesql::prelude::MemoryStorage};

fn main() {
    let storage = MemoryStorage::default();
    let output = std::io::stdout();
    let mut cli = Cli::new(storage, output);

    if let Err(e) = cli.run() {
        eprintln!("{}", e);
    }
}
