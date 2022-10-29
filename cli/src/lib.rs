#![deny(clippy::str_to_string)]

use futures::executor::block_on;
use std::fs::File;
use std::io::{Result as IOResult, Write};

use gluesql_core::result::{Error, Result};
use gluesql_core::store::Store;

mod cli;
mod command;
mod helper;
mod print;

use {
    crate::cli::Cli,
    clap::Parser,
    gluesql_core::store::{GStore, GStoreMut},
    gluesql_memory_storage::MemoryStorage,
    gluesql_sled_storage::SledStorage,
    std::{fmt::Debug, path::PathBuf},
};

#[derive(Parser, Debug)]
#[clap(name = "gluesql", about, version)]
struct Args {
    /// sled-storage path to load
    #[clap(short, long, value_parser)]
    path: Option<PathBuf>,

    /// SQL file to execute
    #[clap(short, long, value_parser)]
    execute: Option<PathBuf>,

    /// PATH to dump whole database
    #[clap(short, long, value_parser)]
    dump: Option<PathBuf>,
}

pub fn run() -> Result<()> {
    let args = Args::parse();

    if let Some(path) = args.path {
        let path = path.as_path().to_str().expect("wrong path");

        if let Some(dump) = args.dump {
            block_on(async {
                let storage = SledStorage::new(path).expect("failed to load sled-storage");
                let schemas = storage.fetch_all_schemas().await?;
                println!("{schemas:?}");
                let ddls = schemas.into_iter().fold("".to_owned(), |acc, schema| {
                    format!("{acc}{}", schema.to_ddl())
                });

                let mut file = File::create(dump).unwrap();
                writeln!(file, "{}\n", ddls);

                Ok::<_, Error>(())
            });

            return Ok(());
        }

        println!("[sled-storage] connected to {}", path);
        run(
            SledStorage::new(path).expect("failed to load sled-storage"),
            args.execute,
        );
    } else {
        println!("[memory-storage] initialized");
        run(MemoryStorage::default(), args.execute);
    }

    fn run<T: GStore + GStoreMut>(storage: T, input: Option<PathBuf>) {
        let output = std::io::stdout();
        let mut cli = Cli::new(storage, output);

        if let Some(path) = input {
            if let Err(e) = cli.load(path.as_path()) {
                println!("[error] {}\n", e);
            };
        }

        if let Err(e) = cli.run() {
            eprintln!("{}", e);
        }
    }

    Ok(())
}
