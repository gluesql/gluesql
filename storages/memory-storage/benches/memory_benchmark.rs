use {
    criterion::{Criterion, criterion_group, criterion_main},
    gluesql_core::prelude::Glue,
    gluesql_memory_storage::MemoryStorage,
};

const ITEM_SIZE: u32 = 5000;

fn seeded_glue() -> Glue<MemoryStorage> {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

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

    glue.execute(&sqls).unwrap();

    glue
}

pub fn bench_insert(c: &mut Criterion) {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    glue.execute(
        "
        CREATE TABLE Testing (
            id INTEGER,
            field_one TEXT,
            field_two TEXT,
            field_three TEXT
        );
    ",
    )
    .unwrap();

    let mut id = 0;

    c.bench_function("insert_one", |b| {
        b.iter(|| {
            let query_str = format!(
                "INSERT INTO Testing
                 VALUES ({:#}, 'Testing 1', 'Testing 2', 'Testing 3');",
                &id
            );
            id += 1;

            glue.execute(&query_str).unwrap();
        });
    });
}

pub fn bench_select(c: &mut Criterion) {
    let mut glue = seeded_glue();

    let mut id = 0;

    // Point-ish lookup via non-indexed equality (exercises full scan + filter).
    c.bench_function("select_one", |b| {
        b.iter(|| {
            let query_str = format!("SELECT * FROM Testing WHERE id = {id}");

            id += 1;
            if id >= ITEM_SIZE {
                id = 1;
            }

            glue.execute(&query_str).unwrap();
        });
    });

    // Range filter over a non-indexed column (full scan + filter).
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

            glue.execute(&query_str).unwrap();
        });
    });
}

pub fn bench_scan(c: &mut Criterion) {
    let mut glue = seeded_glue();

    // Whole-table scan: pure scan_data cost.
    c.bench_function("full_scan", |b| {
        b.iter(|| {
            glue.execute("SELECT * FROM Testing").unwrap();
        });
    });

    // Early-termination case: only one row is needed, yet the current
    // scan_data clones the entire table up front. This is the query where
    // making scan_data lazy should show the largest win.
    c.bench_function("scan_limit_1", |b| {
        b.iter(|| {
            glue.execute("SELECT * FROM Testing LIMIT 1").unwrap();
        });
    });

    // Aggregate over the whole table (full scan, no materialized output rows).
    c.bench_function("count_all", |b| {
        b.iter(|| {
            glue.execute("SELECT COUNT(*) FROM Testing").unwrap();
        });
    });
}

criterion_group!(benches, bench_insert, bench_select, bench_scan);
criterion_main!(benches);
