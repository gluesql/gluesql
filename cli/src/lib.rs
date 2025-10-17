#![deny(clippy::str_to_string)]

mod cli;
mod command;
mod helper;
mod print;

use {
    crate::cli::Cli,
    anyhow::Result,
    clap::Parser,
    futures::{
        executor::block_on,
        stream::{StreamExt, TryStreamExt},
    },
    gluesql_core::{
        ast::{Expr, ToSql},
        data::Value,
        store::{DataRow, GStore, GStoreMut},
    },
    gluesql_csv_storage::CsvStorage,
    gluesql_file_storage::FileStorage,
    gluesql_json_storage::JsonStorage,
    gluesql_memory_storage::MemoryStorage,
    gluesql_parquet_storage::ParquetStorage,
    gluesql_redb_storage::RedbStorage,
    gluesql_sled_storage::SledStorage,
    std::{
        fmt::Debug,
        fs::File,
        io::Write,
        path::{Path, PathBuf},
    },
};

#[derive(Parser, Debug)]
#[clap(name = "gluesql", about, version)]
struct Args {
    /// SQL file to execute
    #[clap(short, long, value_parser)]
    execute: Option<PathBuf>,

    /// PATH to dump whole database
    #[clap(short, long, value_parser)]
    dump: Option<PathBuf>,

    /// Storage type to store data, default is memory
    #[clap(short, long, value_parser)]
    storage: Option<Storage>,

    /// Storage path to load
    #[clap(short, long, value_parser)]
    path: Option<PathBuf>,
}

#[derive(clap::ValueEnum, Debug, Clone, strum_macros::Display)]
#[strum(serialize_all = "lowercase")]
enum Storage {
    Memory,
    Sled,
    Redb,
    Json,
    Csv,
    Parquet,
    File,
}

pub fn run() -> Result<()> {
    let args = Args::parse();
    let path = args.path.as_deref().and_then(Path::to_str);

    match (path, args.storage, args.dump) {
        (None, None, _) | (None, Some(Storage::Memory), _) => {
            println!("[memory-storage] initialized");

            run(MemoryStorage::default(), args.execute);
        }
        (Some(_), Some(Storage::Memory), _) => {
            panic!("failed to load memory-storage: it should be without path");
        }
        (Some(path), Some(storage), Some(dump_path)) => {
            println!("[{}-storage] connected to {}", storage, path);

            dump_database(storage, Some(path), dump_path)?;
        }
        (Some(path), Some(Storage::Sled), _) => {
            println!("[sled-storage] connected to {path}");

            run(
                SledStorage::new(path).expect("failed to load sled-storage"),
                args.execute,
            );
        }
        (Some(path), Some(Storage::Redb), _) => {
            println!("[redb-storage] connected to {path}");

            run(
                RedbStorage::new(path).expect("failed to load redb-storage"),
                args.execute,
            );
        }
        (Some(path), Some(Storage::Json), _) => {
            println!("[json-storage] connected to {path}");

            run(
                JsonStorage::new(path).expect("failed to load json-storage"),
                args.execute,
            );
        }
        (Some(path), Some(Storage::Csv), _) => {
            println!("[csv-storage] connected to {path}");

            run(
                CsvStorage::new(path).expect("failed to load csv-storage"),
                args.execute,
            );
        }
        (Some(path), Some(Storage::Parquet), _) => {
            println!("[parquet-storage] connected to {path}");

            run(
                ParquetStorage::new(path).expect("failed to load parquet-storage"),
                args.execute,
            );
        }
        (Some(path), Some(Storage::File), _) => {
            println!("[file-storage] connected to {path}");

            run(
                FileStorage::new(path).expect("failed to load file-storage"),
                args.execute,
            );
        }
        (None, Some(_), _) | (Some(_), None, _) => {
            panic!("both path and storage should be specified");
        }
    }

    fn run<T: GStore + GStoreMut>(storage: T, input: Option<PathBuf>) {
        let output = std::io::stdout();
        let mut cli = Cli::new(storage, output);

        if let Some(path) = input
            && let Err(e) = cli.load(path.as_path())
        {
            println!("[error] {e}\n");
        }

        if let Err(e) = cli.run() {
            eprintln!("{e}");
        }
    }

    Ok(())
}

pub fn dump_storage<T: GStore + GStoreMut>(storage: &mut T, dump_path: PathBuf) -> Result<()> {
    let file = File::create(dump_path)?;

    block_on(async {
        storage.begin(true).await?;
        let schemas = storage.fetch_all_schemas().await?;
        for schema in schemas {
            writeln!(&file, "{}", schema.to_ddl())?;

            let mut rows_list = storage
                .scan_data(&schema.table_name)
                .await?
                .map_ok(|(_, row)| row)
                .chunks(100);

            while let Some(rows) = rows_list.next().await {
                let exprs_list = rows
                    .into_iter()
                    .map(|result| {
                        result.and_then(|data_row| {
                            let values = match data_row {
                                DataRow::Vec(values) => values,
                                DataRow::Map(values) => vec![Value::Map(values)],
                            };

                            values
                                .into_iter()
                                .map(Expr::try_from)
                                .collect::<std::result::Result<Vec<_>, _>>()
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                let values = exprs_list
                    .into_iter()
                    .map(|exprs| {
                        let row = exprs
                            .into_iter()
                            .map(|expr| expr.to_sql())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({row})")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let insert_statement =
                    format!(r#"INSERT INTO "{}" VALUES {values};"#, schema.table_name);

                writeln!(&file, "{insert_statement}")?;
            }

            writeln!(&file)?;
        }

        Ok(())
    })
}

fn dump_database(storage: Storage, path: Option<&str>, dump_path: PathBuf) -> Result<()> {
    match storage {
        Storage::Memory => {
            if path.is_some() {
                panic!("failed to load memory-storage: it should be without path");
            }

            let mut storage = MemoryStorage::default();
            dump_storage(&mut storage, dump_path)
        }
        Storage::Sled => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = SledStorage::new(path).expect("failed to load sled-storage");
            dump_storage(&mut storage, dump_path)
        }
        Storage::Redb => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = RedbStorage::new(path).expect("failed to load redb-storage");
            dump_storage(&mut storage, dump_path)
        }
        Storage::Json => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = JsonStorage::new(path).expect("failed to load json-storage");
            dump_storage(&mut storage, dump_path)
        }
        Storage::Csv => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = CsvStorage::new(path).expect("failed to load csv-storage");
            dump_storage(&mut storage, dump_path)
        }
        Storage::Parquet => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = ParquetStorage::new(path).expect("failed to load parquet-storage");
            dump_storage(&mut storage, dump_path)
        }
        Storage::File => {
            let path = path.expect("both path and storage should be specified");
            let mut storage = FileStorage::new(path).expect("failed to load file-storage");
            dump_storage(&mut storage, dump_path)
        }
    }
}
