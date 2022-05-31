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
    #[clap(short, long, parse(from_os_str))]
    path: Option<PathBuf>,

    /// SQL file to execute
    #[clap(short, long, parse(from_os_str))]
    execute: Option<PathBuf>,
}

pub fn run() {
    let args = Args::parse();

    if let Some(path) = args.path {
        let path = path.as_path().to_str().expect("wrong path");

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
}
