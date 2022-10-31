#![deny(clippy::str_to_string)]

use bigdecimal::{BigDecimal, FromPrimitive};
use futures::{executor::block_on, stream, StreamExt, TryStream, TryStreamExt};
use gluesql_core::ast::{AstLiteral, Expr, ToSql};
// use std::str::FromStr;
// use gluesql_core::data::BigDecimalExt;
use gluesql_core::prelude::Value;
use gluesql_core::{
    ast::{SetExpr, Statement, Values},
    data::Literal,
    prelude::Row,
    store::Transaction,
};
use std::error::Error;
use std::fs::File;
use std::future::ready;
use std::io::Write;

// use gluesql_core::result::{Error, Result};
use gluesql_core::store::Store;
use std::result::Result;

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

pub fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    if let Some(path) = args.path {
        let path = path.as_path().to_str().expect("wrong path");

        if let Some(dump) = args.dump {
            let storage = SledStorage::new(path).expect("failed to load sled-storage");
            let (storage, schemas) = block_on(async {
                let (storage, _) = storage.begin(true).await.map_err(|(_, error)| error)?;
                let schemas = storage.fetch_all_schemas().await?;

                // let rows_list = schemas.iter().map(|schema| async {
                //     let rows = storage
                //         .scan_data(&schema.table_name)
                //         .await
                //         .unwrap()
                //         .map_ok(|(_, row)| row);

                //     rows
                // });
                // storage.commit().await.map_err(|(_, error)| error)?;

                Ok::<_, Box<dyn Error>>((storage, schemas))
            })?;

            // let ddls = schemas.iter().fold("".to_owned(), |acc, schema| {
            //     let ddl = schema.to_ddl();

            //     format!("{acc}{ddl}")
            // });

            let mut file = File::create(dump)?;
            // writeln!(file, "{}\n", ddls)?;

            let mut insert_statements = schemas.iter().map(|schema| async {
                println!("here:+:+:+:+");
                let insert_statements = storage
                    .scan_data(&schema.table_name)
                    .await
                    .map(stream::iter)?
                    .map_ok(|(_, row)| row)
                    .try_chunks(100)
                    .map_ok(|rows| {
                        let exprs_list = rows
                            .into_iter()
                            .map(|Row(values)| {
                                values
                                    .into_iter()
                                    .map(|value| match value {
                                        Value::Bool(v) => Expr::Literal(AstLiteral::Boolean(v)),
                                        Value::I8(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_i8(v).unwrap(),
                                        )),
                                        Value::I16(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_i16(v).unwrap(),
                                        )),
                                        Value::I32(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_i32(v).unwrap(),
                                        )),
                                        Value::I64(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_i64(v).unwrap(),
                                        )),
                                        Value::I128(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_i128(v).unwrap(),
                                        )),
                                        Value::U8(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_u8(v).unwrap(),
                                        )),
                                        Value::F64(v) => Expr::Literal(AstLiteral::Number(
                                            BigDecimal::from_f64(v).unwrap(),
                                        )),
                                        Value::Decimal(v) => todo!(),
                                        Value::Str(v) => Expr::Literal(AstLiteral::QuotedString(v)),
                                        Value::Bytea(v) => todo!(),
                                        Value::Date(v) => todo!(),
                                        Value::Timestamp(v) => todo!(),
                                        Value::Time(v) => todo!(),
                                        Value::Interval(v) => todo!(),
                                        Value::Uuid(v) => todo!(),
                                        Value::Map(v) => todo!(),
                                        Value::List(v) => todo!(),
                                        Value::Null => Expr::Literal(AstLiteral::Null),
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>();

                        let stmt = Statement::Insert {
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

                        println!(":+:+:+::+:+:+:+:+:+:");
                        println!("{stmt}");
                        writeln!(&file, "{}\n", format!("{stmt};\n"));
                    });
                // .try_fold("".to_owned(), |acc, cur| async move {
                //     writeln!(file, "{}\n", format!("{cur};\n"));

                //     Ok(format!("{acc}{cur};\n"))
                // });

                Ok::<_, Box<dyn Error>>(insert_statements)
            });

            insert_statements.next();

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
