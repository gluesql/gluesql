#![deny(clippy::str_to_string)]

use futures::stream::{iter, TryChunksError};
use itertools::Itertools;

mod cli;
mod command;
mod helper;
mod print;

use {
    crate::cli::Cli,
    anyhow::{Error, Result},
    clap::Parser,
    futures::{executor::block_on, stream, StreamExt, TryStreamExt},
    gluesql_core::{
        ast::{AstLiteral, Expr, SetExpr, Statement, ToSql, Values},
        prelude::Row,
        store::Transaction,
        store::{GStore, GStoreMut, Store},
    },
    gluesql_memory_storage::MemoryStorage,
    gluesql_sled_storage::SledStorage,
    std::{fmt::Debug, fs::File, future::ready, io::Write, path::PathBuf},
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
            let file = File::create(dump)?;
            let storage = SledStorage::new(path).expect("failed to load sled-storage");
            block_on(async {
                let (storage, _) = storage.begin(true).await.map_err(|(_, error)| error)?;
                let schemas = storage.fetch_all_schemas().await?;
                stream::iter(&schemas)
                    // schemas
                    // .iter()
                    .for_each(|schema| async {
                        writeln!(&file, "{}", schema.clone().to_ddl());

                        let rows_list = storage
                            .scan_data(&schema.table_name)
                            .await
                            // .map(stream::iter)
                            .unwrap()
                            .map(|result| result.map(|(_, row)| row))
                            .chunks(100);
                        // .try_chunks(100)
                        // .for_each(|rows| {
                        for rows in &rows_list {
                            let exprs_list = rows
                                .map_ok(|Row(values)| {
                                    values
                                        .into_iter()
                                        .map(|value| {
                                            Ok(Expr::Literal(AstLiteral::try_from(
                                                value.to_owned(),
                                            )?))
                                        })
                                        .collect::<Result<Vec<_>>>()
                                        .unwrap()
                                })
                                // .collect();
                                .collect::<Result<Vec<_>, _>>()
                                .unwrap();
                            // .map_err(|err| TryChunksError(rows, err))?;
                            // .map_err(|err| Err(ready(TryChunksError(rows, err))))?;

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

                            writeln!(&file, "{}", insert_statement)
                                .map_err(ready)
                                .unwrap();

                            // ready(Ok::<_, Error>(()))
                            // Ok::<_, Error>(())
                        }
                        // .await
                        // .unwrap();

                        writeln!(&file).unwrap();

                        // ready(Ok(()))
                        // ready(Ok::<_, Error>(()))
                    })
                    .await;

                Ok::<_, Error>(())
            })?;

            // return Ok(());
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
