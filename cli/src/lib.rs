#![deny(clippy::str_to_string)]
#![allow(deprecated)]

mod cli;
mod command;
mod helper;
mod print;
mod upgrade;

use {
    crate::cli::Cli,
    anyhow::Result,
    clap::Parser,
    gluesql_core::{
        ast::{Expr, ToSql},
        store::{GStore, GStoreMut, Planner, Store, Transaction},
    },
    gluesql_csv_storage::CsvStorage,
    gluesql_file_storage::FileStorage,
    gluesql_json_storage::JsonStorage,
    gluesql_memory_storage::MemoryStorage,
    gluesql_parquet_storage::ParquetStorage,
    gluesql_redb_storage::RedbStorage,
    gluesql_sled_storage::SledStorage,
    std::{fmt::Debug, fs::File, io::Write, path::PathBuf},
};

const SLED_STORAGE_DEPRECATION_WARNING: &str = "[warning] sled-storage is deprecated and will be removed in v0.21.0; use redb-storage for new persistent-storage deployments";

fn warn_sled_storage_deprecated() {
    eprintln!("{SLED_STORAGE_DEPRECATION_WARNING}");
}

#[derive(Parser, Debug)]
#[clap(name = "gluesql", about, version)]
struct Args {
    /// SQL file to execute
    #[clap(short, long, value_parser)]
    execute: Option<PathBuf>,

    /// SQL file to dump the selected storage into
    #[clap(short, long, value_parser, conflicts_with = "execute")]
    dump: Option<PathBuf>,

    /// Storage type to store data; defaults to memory.
    /// sled is deprecated and will be removed in v0.21.0; use redb for new deployments.
    #[clap(short, long, value_parser)]
    storage: Option<Storage>,

    /// Storage path to load
    #[clap(short, long, value_parser, requires = "storage")]
    path: Option<PathBuf>,

    /// Upgrade storage data format to the latest version
    #[clap(
        long,
        requires_all = &["storage", "path"],
        conflicts_with_all = &["execute", "dump"]
    )]
    upgrade: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
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
    fn run<T: GStore + GStoreMut + Planner>(
        mut storage: T,
        input: Option<PathBuf>,
        dump: Option<PathBuf>,
    ) -> Result<()> {
        if let Some(path) = dump {
            return dump_database(&mut storage, path);
        }

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

        Ok(())
    }

    let Args {
        execute,
        dump,
        storage,
        path,
        upgrade,
    } = Args::parse();

    if upgrade {
        return upgrade::run_upgrade(path.as_deref(), storage, execute.is_some(), dump.is_some());
    }

    let path = path.as_deref();

    match (path, storage) {
        (None, None | Some(Storage::Memory)) => {
            println!("[memory-storage] initialized");

            run(MemoryStorage::default(), execute, dump)?;
        }
        (Some(_), Some(Storage::Memory)) => {
            panic!("failed to load memory-storage: it should be without path");
        }
        (Some(path), Some(Storage::Sled)) => {
            warn_sled_storage_deprecated();
            println!("[sled-storage] connected to {}", path.display());

            run(
                SledStorage::new(path).expect("failed to load sled-storage"),
                execute,
                dump,
            )?;
        }
        (Some(path), Some(Storage::Redb)) => {
            println!("[redb-storage] connected to {}", path.display());

            run(
                RedbStorage::new(path).expect("failed to load redb-storage"),
                execute,
                dump,
            )?;
        }
        (Some(path), Some(Storage::Json)) => {
            println!("[json-storage] connected to {}", path.display());

            run(
                JsonStorage::new(path).expect("failed to load json-storage"),
                execute,
                dump,
            )?;
        }
        (Some(path), Some(Storage::Csv)) => {
            println!("[csv-storage] connected to {}", path.display());

            run(
                CsvStorage::new(path).expect("failed to load csv-storage"),
                execute,
                dump,
            )?;
        }
        (Some(path), Some(Storage::Parquet)) => {
            println!("[parquet-storage] connected to {}", path.display());

            run(
                ParquetStorage::new(path).expect("failed to load parquet-storage"),
                execute,
                dump,
            )?;
        }
        (Some(path), Some(Storage::File)) => {
            println!("[file-storage] connected to {}", path.display());

            run(
                FileStorage::new(path).expect("failed to load file-storage"),
                execute,
                dump,
            )?;
        }
        (None, Some(_)) | (Some(_), None) => {
            panic!("both path and storage should be specified");
        }
    }

    Ok(())
}

pub fn dump_database<T: Store + Transaction>(storage: &mut T, dump_path: PathBuf) -> Result<()> {
    let mut file = File::create(dump_path)?;

    let autocommit = storage.begin(true)?;
    let result = write_dump(storage, &mut file);

    if !autocommit {
        return result;
    }

    match result {
        Ok(()) => storage.commit().map_err(Into::into),
        Err(error) => {
            storage.rollback()?;

            Err(error)
        }
    }
}

fn write_dump<T: Store>(storage: &T, file: &mut File) -> Result<()> {
    let schemas = storage.fetch_all_schemas()?;
    for schema in schemas {
        writeln!(file, "{}", schema.to_ddl())?;

        let mut rows = storage
            .scan_data(&schema.table_name)?
            .map(|result| result.map(|(_, row)| row));

        loop {
            let exprs_list = rows
                .by_ref()
                .take(100)
                .map(|result| {
                    result.map(|row| row.into_iter().map(Expr::Value).collect::<Vec<_>>())
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;

            if exprs_list.is_empty() {
                break;
            }

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

            writeln!(file, "{insert_statement}")?;
        }

        writeln!(file)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::{Args, Storage},
        clap::{ErrorKind, Parser},
        std::path::Path,
    };

    #[test]
    fn parse_dump() {
        let actual = Args::try_parse_from([
            "gluesql",
            "--storage",
            "redb",
            "--path",
            "data.redb",
            "--dump",
            "dump.sql",
        ])
        .unwrap();
        assert_eq!(actual.storage, Some(Storage::Redb));
        assert_eq!(actual.path.as_deref(), Some(Path::new("data.redb")));
        assert_eq!(actual.dump.as_deref(), Some(Path::new("dump.sql")));
        assert_eq!(actual.execute, None);
        assert!(!actual.upgrade);
    }

    #[test]
    fn parse_dump_requires_storage_with_path() {
        let actual = Args::try_parse_from(["gluesql", "--path", "data.redb", "--dump", "dump.sql"])
            .unwrap_err();
        assert_eq!(actual.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn parse_dump_rejects_execute() {
        let actual =
            Args::try_parse_from(["gluesql", "--execute", "query.sql", "--dump", "dump.sql"])
                .unwrap_err();
        assert_eq!(actual.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parse_upgrade_requires_storage_and_path() {
        let args = Args::try_parse_from(["gluesql", "--upgrade"]);
        assert!(args.is_err());
    }

    #[test]
    fn parse_upgrade_rejects_execute() {
        let args = Args::try_parse_from([
            "gluesql",
            "--upgrade",
            "--storage",
            "sled",
            "--path",
            "./tmp",
            "--execute",
            "query.sql",
        ]);
        assert!(args.is_err());
    }

    #[test]
    fn parse_upgrade_rejects_dump() {
        let args = Args::try_parse_from([
            "gluesql",
            "--upgrade",
            "--storage",
            "file",
            "--path",
            "./tmp",
            "--dump",
            "dump.sql",
        ])
        .unwrap_err();
        assert_eq!(args.kind(), ErrorKind::ArgumentConflict);
    }
}
