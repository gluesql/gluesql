use criterion::{criterion_group, criterion_main, Criterion};
use gluesql::{parse, Glue, SledStorage};
use std::convert::TryFrom;

// Generate benchmark tests
pub fn bench_insert(c: &mut Criterion) {
    // Generate a new database
    let path = format!("data/bench_insert");

    match std::fs::remove_dir_all(&path) {
        Ok(()) => (),
        Err(e) => {
            println!("fs::remove_file {:?}", e);
        }
    }

    let config = sled::Config::default()
        .path(path)
        .temporary(true)
        .mode(sled::Mode::HighThroughput);

    let storage = SledStorage::try_from(config).unwrap();
    let mut glue = Glue::new(storage);
    // Create a dummy table
    let sqls = "
        CREATE TABLE Testing (
            id INTEGER,
            field_one TEXT,
            field_two TEXT,
            field_three TEXT
        );
    ";

    for query in parse(sqls).unwrap() {
        glue.execute(&query).unwrap();
    }

    // Prepare query out of scope, and copy it at the beginning
    let mut id = 0;

    c.bench_function("insert_one", |b| {
        b.iter(|| {
            let query_str = format!(
                "INSERT INTO Testing \
            VALUES ({:#}, \"Testing 1\", \"Testing 2\", \"Testing 3\");",
                &id
            );
            id += 1;
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
}

pub fn bench_select(c: &mut Criterion) {
    // Generate a new database
    let path = format!("data/bench_select");

    match std::fs::remove_dir_all(&path) {
        Ok(()) => (),
        Err(e) => {
            println!("fs::remove_file {:?}", e);
        }
    }

    let config = sled::Config::default()
        .path(path)
        .temporary(true)
        .mode(sled::Mode::HighThroughput);

    let storage = SledStorage::try_from(config).unwrap();
    let mut glue = Glue::new(storage);
    // Create a dummy table
    {
        let mut sqls: String = "
        CREATE TABLE Testing (
            id INTEGER,
            field_one TEXT,
            field_two TEXT,
            field_three TEXT
        );"
        .to_string();
        // Insert 100k elements
        for i in 0..100000 {
            sqls += &*format!(
                "INSERT INTO Testing \
            VALUES ({:#}, \"Testing 1\", \"Testing 2\", \"Testing 3\");",
                &i
            );
        }

        for query in parse(&sqls).unwrap() {
            glue.execute(&query).unwrap();
        }
    }

    // Prepare query out of scope, and copy it at the beginning
    let mut id = 0;

    c.bench_function("select_one", |b| {
        b.iter(|| {
            let query_str = format!("SELECT * FROM Testing WHERE id = {}", id);
            id += 1;
            if id >= 10000 {
                id = 1;
            }
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_insert, bench_select);
criterion_main!(benches);
