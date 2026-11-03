//! Run against an archived release checkout, not the current storage crate.
//! See README.md for the fixture-writer manifest and feature flags.

use {
    gluesql_core::{
        ast::Expr,
        data::{Schema, Value},
        prelude::Glue,
        store::{StoreMut, Transaction},
    },
    gluesql_redb_storage::RedbStorage,
    redb::{Database, ReadableTable, TableDefinition},
    std::{env, error::Error, fs, path::Path},
};

#[cfg(feature = "legacy-async")]
macro_rules! call {
    ($expression:expr) => {
        $expression.await
    };
}
#[cfg(not(feature = "legacy-async"))]
macro_rules! call {
    ($expression:expr) => {
        $expression
    };
}

fn main() -> Result<(), Box<dyn Error>> {
    futures::executor::block_on(run())
}

// A v2 route fixture keeps released v1 schema bytes and the upgraded row bytes.
// It does not introduce a schema layout from an unreleased development commit.
fn copy_as_v2(source: &Path, path: &Path) -> Result<(), Box<dyn Error>> {
    let source = Database::open(source)?;
    let read = source.begin_read()?;
    let schemas = TableDefinition::<&str, Vec<u8>>::new("__SCHEMA__");
    let mut db = Database::create(path)?;
    let write = db.begin_write()?;
    let mut names = Vec::new();
    {
        let mut target = write.open_table(schemas)?;
        for entry in read.open_table(schemas)?.iter()? {
            let (name, payload) = entry?;
            target.insert(name.value(), payload.value())?;
            names.push(name.value().to_owned());
        }
    }
    for name in names {
        let definition = TableDefinition::<&[u8], Vec<u8>>::new(&name);
        let mut target = write.open_table(definition)?;
        for entry in read.open_table(definition)?.iter()? {
            let (key, payload) = entry?;
            target.insert(key.value(), payload.value())?;
        }
    }
    write
        .open_table(TableDefinition::<&str, u32>::new("__GLUESQL_META__"))?
        .insert("storage_format_version", &2)?;
    write.commit()?;
    db.compact()?;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().collect();
    let path = Path::new(&args[1]);
    let mode = &args[2];
    fs::create_dir_all(path.parent().unwrap())?;
    if mode == "v2" {
        return copy_as_v2(Path::new(&args[3]), path);
    }
    #[cfg(not(feature = "legacy-async"))]
    if mode == "upgrade" {
        println!("{:?}", gluesql_redb_storage::migrate_to_latest(path)?);
        Database::open(path)?.compact()?;
        return Ok(());
    }
    if mode == "index-payload" {
        // Reproduce the fixed schema payload used by the schema conversion unit.
        // This mode prints bytes and does not create a database file.
        let mut schema = Schema::from_ddl(
            "CREATE TABLE Queries (id INT); CREATE INDEX expression_index ON Queries (id + 1);",
        )?;
        schema.indexes[0].created = "1970-01-01T00:00:00".parse()?;
        println!("{:?}", bincode::serialize(&schema)?);
        return Ok(());
    }
    let mut glue = Glue::new(RedbStorage::new(path)?);
    match mode.as_str() {
        "defaults" => {
            call!(glue.execute(
                "CREATE TABLE Defaults (
                    id INT PRIMARY KEY,
                    label TEXT DEFAULT 'legacy-default',
                    score INT DEFAULT (2 + 3),
                    ready BOOLEAN DEFAULT NOT FALSE,
                    pattern BOOLEAN DEFAULT 'abc' LIKE 'a%'
                ); INSERT INTO Defaults (id) VALUES (1);"
            ))?;
            println!("{:?}", call!(glue.execute("SELECT * FROM Defaults;"))?);
        }
        "modern" => {
            call!(glue.execute(
                "CREATE TABLE Modern (
                    id INT PRIMARY KEY,
                    label TEXT DEFAULT 'modern-default',
                    matches BOOLEAN DEFAULT 'abc' ~ 'a.*'
                ); INSERT INTO Modern (id) VALUES (1);"
            ))?;
            println!("{:?}", call!(glue.execute("SELECT * FROM Modern;"))?);
        }
        "ambiguous" => {
            call!(glue.execute(
                "CREATE TABLE Ambiguous (
                    id INT PRIMARY KEY,
                    flag BOOLEAN DEFAULT TEXT '2' BETWEEN TEXT '10' AND TEXT '9'
                ); INSERT INTO Ambiguous (id) VALUES (1);"
            ))?;
            println!("{:?}", call!(glue.execute("SELECT * FROM Ambiguous;"))?);
        }
        "decimal" => {
            let mut schema =
                Schema::from_ddl("CREATE TABLE Ambiguous (id INT PRIMARY KEY, flag BOOLEAN);")?;
            schema.column_defs.as_mut().unwrap()[1].default = Some(Expr::Between {
                expr: Box::new(Expr::Value(Value::Decimal("2".parse()?))),
                negated: false,
                low: Box::new(Expr::Value(Value::Decimal("10".parse()?))),
                high: Box::new(Expr::Value(Value::Decimal("9".parse()?))),
            });
            call!(glue.storage.begin(false))?;
            call!(glue.storage.insert_schema(&schema))?;
            call!(glue.storage.commit())?;
            call!(glue.execute("INSERT INTO Ambiguous (id) VALUES (1);"))?;
            println!("{:?}", call!(glue.execute("SELECT * FROM Ambiguous;"))?);
        }
        #[cfg(feature = "legacy-async")]
        "index-plan" | "hash-join" => {
            // Public StoreMut can persist these ASTs even though SQL DEFAULT
            // evaluation does not support executing subqueries.
            let ddl = "CREATE TABLE Queries (
                id INT,
                value INT DEFAULT (
                    SELECT SUM(A.id + 1) FROM Numbers AS A
                    INNER JOIN Numbers AS B ON A.id = B.id
                    WHERE A.id > 1 GROUP BY A.id
                    HAVING SUM(A.id) > 1 ORDER BY A.id LIMIT 3 OFFSET 1
                )
            );";
            let mut schema = Schema::from_ddl(ddl)?;
            {
                use gluesql_core::ast::{
                    IndexItem, JoinConstraint, JoinExecutor, JoinOperator, SetExpr, TableFactor,
                };
                let Some(Expr::Subquery(query)) =
                    schema.column_defs.as_mut().unwrap()[1].default.as_mut()
                else {
                    return Err("expected subquery".into());
                };
                let SetExpr::Select(select) = &mut query.body else {
                    return Err("expected SELECT".into());
                };
                if mode == "index-plan" {
                    let TableFactor::Table { index, .. } = &mut select.from.relation else {
                        return Err("expected table".into());
                    };
                    *index = Some(IndexItem::PrimaryKey(Expr::Value(Value::I64(2))));
                    select.selection = None;
                } else {
                    let join = &mut select.from.joins[0];
                    let JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                        left, right, ..
                    })) = &join.join_operator
                    else {
                        return Err("expected equality join".into());
                    };
                    join.join_executor = JoinExecutor::Hash {
                        key_expr: *left.clone(),
                        value_expr: *right.clone(),
                        where_clause: None,
                    };
                    join.join_operator = JoinOperator::Inner(JoinConstraint::None);
                }
            }
            call!(glue.storage.begin(false))?;
            call!(glue.storage.insert_schema(&schema))?;
            call!(glue.storage.commit())?;
        }
        _ => return Err("unknown fixture mode".into()),
    }
    drop(glue);
    // Keep the checked-in fixture small without changing its logical version
    // or redb file format. Never run upgrade() here.
    Database::open(path)?.compact()?;
    Ok(())
}
