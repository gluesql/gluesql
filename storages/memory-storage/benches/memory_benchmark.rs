use {
    criterion::{BatchSize, Criterion, criterion_group, criterion_main},
    gluesql_core::prelude::Glue,
    gluesql_memory_storage::MemoryStorage,
};

const ITEM_SIZE: u32 = 5000;

/// Width of the range selected by `select_many`.
const RANGE: u32 = 50;

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
    // Insert into a fresh single-table database each iteration so the measured
    // workload stays constant. Growing the table across iterations would make
    // the timing depend on how many inserts ran before, not on the insert cost.
    c.bench_function("insert_one", |b| {
        b.iter_batched(
            || {
                let mut glue = Glue::new(MemoryStorage::default());
                glue.execute(
                    "CREATE TABLE Testing (
                        id INTEGER,
                        field_one TEXT,
                        field_two TEXT,
                        field_three TEXT
                    );",
                )
                .unwrap();
                glue
            },
            |mut glue| {
                glue.execute(
                    "INSERT INTO Testing
                     VALUES (0, 'Testing 1', 'Testing 2', 'Testing 3');",
                )
                .unwrap();
            },
            BatchSize::SmallInput,
        );
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

    // Range filter over a non-indexed column (full scan + filter). Use a
    // dedicated counter that wraps before the range would run past the last
    // row, so every iteration selects the same number of rows (a constant
    // window of `RANGE`) instead of shrinking near the top of the table.
    let mut range_start = 0;
    c.bench_function("select_many", |b| {
        b.iter(|| {
            let query_str = format!(
                "SELECT * FROM Testing WHERE id > {} AND id < {}",
                range_start,
                range_start + RANGE
            );

            range_start += 1;
            if range_start >= ITEM_SIZE - RANGE {
                range_start = 0;
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

    // Early-termination case: only one row is needed. With the lazy scan_data,
    // only the single row consumed by `LIMIT 1` is cloned instead of the whole
    // table, so this is the query where the optimization shows the largest win.
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
