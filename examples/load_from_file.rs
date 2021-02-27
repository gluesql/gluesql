// Glue TODO: Clean up, probably redo.
// Note: Basic and unclean example.
// Note: Untested after edits, should just be used as a gist.

/* Cargo.toml
[dependencies]
//(gluesql)
csv = "1"
fstrings = "0.2" // Optional
*/

use {
    crate::db_util,
    csv::StringRecord,
    fstrings::*,
    gluesql::{parse, Glue, Payload, SledStorage},
};

fn main() {
    println!(
        "{:?}",
        read("/files/gluesql/load_from_file.csv", "file_table")
    );
}

fn read(path: &str, table: &str) -> std::result::Result<(), ()> {
    let storage = SledStorage::new("/tmp/gluesql/load_from_file").expect("Something went wrong!");
    let mut db = Glue::new(storage);

    let mut records = csv::ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_path(path)
        .unwrap()
        .records()
        .map(convert)
        .collect::<Vec<StringRecord>>();

    let columns = db_util::string_record_to_vector(&records[0])
        .iter()
        .map(|record| {
            let column = regex::Regex::new(r"[^A-z|0-9]")
                .unwrap()
                .replace_all(record, "_");
            f!("{column} TEXT,")
        })
        .collect::<Vec<String>>()
        .join("");
    let table_alter = db.execute(&f!("DROP TABLE {table}; CREATE TABLE {table} ({columns});"));

    // Get rid of header record
    records.drain(0..1);

    insert_string_records(db, table, records)
}

fn convert(record: Result<StringRecord, csv::Error>) -> StringRecord {
    return match record {
        Ok(record) => return record,
        Err(_) => Default::default(),
    };
}

// Glue TODO: Add API for doing this
fn insert_string_records(
    db: Glue,
    table: String,
    records: Vec<csv::StringRecord>,
) -> gluesql::Result<Payload> {
    insert(
        db,
        table,
        records
            .iter()
            .map(|record| {
                string_record_to_vector(&record)
                    .iter()
                    .map(|string| {
                        Expr::Identifier(Ident {
                            value: string.to_string(),
                            quote_style: None,
                        })
                    })
                    .collect()
            })
            .collect(),
        vec![],
    )
}

fn insert(
    db: Glue,
    table: String,
    rows: Vec<Vec<gluesql::parser::ast::Expr>>,
    columns: Vec<Ident>,
) -> gluesql::Result<Payload> {
    db.execute(&gluesql::Query(Statement::Insert {
        table_name: ObjectName(
            table
                .split(".")
                .map(|table_part| Ident {
                    value: table_part.to_string(),
                    quote_style: None,
                })
                .collect(),
        ),
        columns,
        source: Box::new(gluesql::parser::ast::Query {
            ctes: vec![],
            body: SetExpr::Values(Values(rows)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }))
}
