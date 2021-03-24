use criterion::{criterion_group, criterion_main, Criterion};
use gluesql::{parse, Glue, SledStorage};
use std::convert::TryFrom;

const ITEM_SIZE: u32 = 5000;

// Generate benchmark tests
pub fn bench_insert(c: &mut Criterion) {
    // Generate a new database
    let path = format!("data/bench_insert");

    match std::fs::remove_dir_all(&path) {
        Ok(()) => (),
        // Silently ignore, 99% of the time this will already be removed.
        Err(_) => (),
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
        // Silently ignore, 99% of the time this will already be removed.
        Err(_) => (),
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

        for i in 0..ITEM_SIZE {
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
            if id >= ITEM_SIZE {
                id = 1;
            }
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
    c.bench_function("select_many", |b| {
        b.iter(|| {
            let query_str = format!(
                "SELECT * FROM Testing WHERE id > {} AND id < {}",
                id,
                id + 50
            );
            id += 1;
            if id >= ITEM_SIZE {
                id = 1;
            }
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
}

pub fn bench_select_tainted(c: &mut Criterion) {
    // Generate a new database
    let path = format!("data/bench_select_tainted");

    match std::fs::remove_dir_all(&path) {
        Ok(()) => (),
        // Silently ignore, 99% of the time this will already be removed.
        Err(_) => (),
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
        );
        CREATE TABLE TestingTainted (
            id INTEGER,
            field_one TEXT,
            field_two TEXT,
            field_three TEXT
        );
        "
        .to_string();

        for i in 0..ITEM_SIZE {
            sqls += &*format!(
                "INSERT INTO Testing \
            VALUES ({0:#}, \"Testing 1\", \"Testing 2\", \"Testing 3\");\
                INSERT INTO TestingTainted \
            VALUES ({0:#}, \"Testing_tainted 1\", \"Testing_tainted 2\", \"Testing_tainted 3\");",
                &i
            );
        }

        for query in parse(&sqls).unwrap() {
            glue.execute(&query).unwrap();
        }
    }

    // Prepare query out of scope, and copy it at the beginning
    let mut id = 0;

    c.bench_function("select_one_tainted", |b| {
        b.iter(|| {
            let query_str = format!("SELECT * FROM Testing WHERE id = {}", id);
            id += 1;
            if id >= ITEM_SIZE {
                id = 1;
            }
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
    c.bench_function("select_many_tainted", |b| {
        b.iter(|| {
            let query_str = format!(
                "SELECT * FROM Testing WHERE id > {} AND id < {}",
                id,
                id + 50
            );
            id += 1;
            if id >= ITEM_SIZE {
                id = 1;
            }
            for query in parse(&query_str).unwrap() {
                glue.execute(&query).unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_insert, bench_select, bench_select_tainted);
criterion_main!(benches);
