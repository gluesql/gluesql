#![deny(clippy::str_to_string)]

mod cli;
mod command;
mod helper;
mod print;

use {
    crate::cli::Cli,
    anyhow::{Error, Result},
    clap::Parser,
    futures::executor::block_on,
    gluesql_core::{
        ast::{Expr, SetExpr, Statement, ToSql, Values},
        store::{GStore, GStoreMut, Store, Transaction, DataRow},
    },
    gluesql_memory_storage::MemoryStorage,
    gluesql_sled_storage::SledStorage,
    itertools::Itertools,
    std::{fmt::Debug, fs::File, io::Write, path::PathBuf},
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

        if let Some(dump_path) = args.dump {
            let storage = SledStorage::new(path).expect("failed to load sled-storage");
            dump_database(storage, dump_path)?;

            return Ok::<_, Error>(());
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

pub fn dump_database(storage: SledStorage, dump_path: PathBuf) -> Result<SledStorage> {
    let file = File::create(dump_path)?;

    block_on(async {
        let (storage, _) = storage.begin(true).await.map_err(|(_, error)| error)?;
        let schemas = storage.fetch_all_schemas().await?;
        for schema in schemas {
            writeln!(&file, "{}", schema.clone().to_ddl())?;

            let rows_list = storage
                .scan_data(&schema.table_name)
                .await?
                .map_ok(|(_, row)| row)
                .chunks(100);

            for rows in &rows_list {
                let exprs_list = rows
                    .map(|result| {
                        result.map(|data_row| {
                            let values = match data_row {
                                DataRow::Vec(values) => values,
                                DataRow::Map(_) => todo!(),
                            };

                            values
                                .into_iter()
                                .map(|value| Ok(Expr::try_from(value)?))
                                .collect::<Result<Vec<_>>>()
                        })?
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let insert_statement = Statement::Insert {
                    table_name: schema.table_name.clone(),
                    columns: Vec::new(),
                    source: gluesql_core::ast::Query {
                        body: SetExpr::Values(Values(exprs_list)),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                    },
                }
                .to_sql();

                writeln!(&file, "{}", insert_statement)?;
            }

            writeln!(&file)?;
        }

        Ok(storage)
    })
}
