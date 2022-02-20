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
}

pub fn run() {
    let args = Args::parse();

    if let Some(path) = args.path {
        let path = path.as_path().to_str().expect("wrong path");

        println!("[sled-storage] connected to {}", path);
        run(SledStorage::new(path).expect("failed to load sled-storage"));
    } else {
        println!("[memory-storage] initialized");
        run(MemoryStorage::default());
    }

    fn run<T, U: GStore<T> + GStoreMut<T>>(storage: U) {
        let output = std::io::stdout();
        let mut cli = Cli::new(storage, output);

        if let Err(e) = cli.run() {
            eprintln!("{}", e);
        }
    }
}
