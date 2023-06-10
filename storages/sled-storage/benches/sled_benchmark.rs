use {
    criterion::{criterion_group, criterion_main, Criterion},
    futures::executor::block_on,
    gluesql_core::prelude::Glue,
    gluesql_sled_storage::SledStorage,
};

const ITEM_SIZE: u32 = 5000;

// Generate benchmark tests
pub fn bench_insert(c: &mut Criterion) {
    // Generate a new database
    let path = "data/bench_insert";

    // Silently ignore, 99% of the time this will already be removed
    let _ = std::fs::remove_dir_all(path);

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

    block_on(glue.execute(sqls)).unwrap();

    // Prepare query out of scope, and copy it at the beginning
    let mut id = 0;

    c.bench_function("insert_one", |b| {
        b.iter(|| {
            let query_str = format!(
                "INSERT INTO Testing 
                 VALUES ({:#}, 'Testing 1', 'Testing 2', 'Testing 3');",
                &id
            );
            id += 1;

            block_on(glue.execute(&query_str)).unwrap();
        })
    });
}

pub fn bench_select(c: &mut Criterion) {
    // Generate a new database
    let path = "data/bench_select";

    // Silently ignore, 99% of the time this will already be removed
    let _ = std::fs::remove_dir_all(path);

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
        .to_owned();

        for i in 0..ITEM_SIZE {
            sqls += &*format!(
                "INSERT INTO Testing
                 VALUES ({:#}, 'Testing 1', 'Testing 2', 'Testing 3');",
                &i
            );
        }

        block_on(glue.execute(&sqls)).unwrap();
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

            block_on(glue.execute(&query_str)).unwrap();
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

            block_on(glue.execute(&query_str)).unwrap();
        })
    });
}

pub fn bench_select_tainted(c: &mut Criterion) {
    // Generate a new database
    let path = "data/bench_select_tainted";

    // Silently ignore, 99% of the time this will already be removed
    let _ = std::fs::remove_dir_all(path);

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
        .to_owned();

        for i in 0..ITEM_SIZE {
            sqls += &*format!(
                "INSERT INTO Testing
                 VALUES ({0:#}, 'Testing 1', 'Testing 2', 'Testing 3');
                 INSERT INTO TestingTainted
                 VALUES ({0:#}, 'Testing_tainted 1', 'Testing_tainted 2', 'Testing_tainted 3');",
                &i
            );
        }

        block_on(glue.execute(&sqls)).unwrap();
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

            block_on(glue.execute(&query_str)).unwrap();
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

            block_on(glue.execute(&query_str)).unwrap();
        })
    });
}

criterion_group!(benches, bench_insert, bench_select, bench_select_tainted);
criterion_main!(benches);
