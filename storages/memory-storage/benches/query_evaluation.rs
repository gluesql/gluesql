use {
    criterion::{Criterion, criterion_group, criterion_main},
    gluesql_core::{
        data::Value,
        prelude::{Glue, Payload},
        store::StoreMut,
    },
    gluesql_memory_storage::MemoryStorage,
    std::{collections::BTreeMap, hint::black_box},
};

const ROW_COUNT: i64 = 1_000;

fn inputs() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        ("i64", "INTEGER", Value::I64(42)),
        ("string_256", "TEXT", Value::Str("x".repeat(256))),
        ("string_4096", "TEXT", Value::Str("x".repeat(4_096))),
        (
            "map_8x256",
            "MAP",
            Value::Map(
                (0..8)
                    .map(|index| (format!("key_{index:04}"), Value::Str("x".repeat(256))))
                    .collect::<BTreeMap<_, _>>(),
            ),
        ),
    ]
}

fn validate_result(payload: Payload, operation: &str, value: &Value) {
    let Payload::Select { labels, rows } = payload else {
        panic!("expected SELECT result");
    };
    let columns = if operation == "repeated_projection" {
        2
    } else {
        1
    };
    assert_eq!(labels.len(), columns);
    if matches!(operation, "integer_filter" | "payload_equality") {
        let start = if operation == "integer_filter" {
            ROW_COUNT / 2
        } else {
            0
        };
        let mut actual = rows
            .into_iter()
            .map(|row| {
                assert_eq!(row.len(), 1);
                let Value::I64(id) = row[0] else {
                    panic!("expected integer id");
                };
                id
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, (start..ROW_COUNT).collect::<Vec<_>>());
    } else {
        assert_eq!(rows.len(), usize::try_from(ROW_COUNT).unwrap());
        for row in rows {
            assert_eq!(row.len(), columns);
            assert!(row.iter().all(|column| column == value));
        }
    }
}

fn bench_query_evaluation(c: &mut Criterion) {
    for (name, data_type, value) in inputs() {
        let mut glue = Glue::new(MemoryStorage::default());
        glue.execute(format!(
            "CREATE TABLE bench (id INTEGER, payload {data_type})"
        ))
        .unwrap();
        let rows = (0..ROW_COUNT)
            .map(|id| vec![Value::I64(id), value.clone()])
            .collect();
        glue.storage.append_data("bench", rows).unwrap();

        for (operation, sql) in [
            ("integer_filter", "SELECT id FROM bench WHERE id >= 500"),
            ("projection", "SELECT payload FROM bench"),
            ("repeated_projection", "SELECT payload, payload FROM bench"),
            (
                "payload_equality",
                "SELECT id FROM bench WHERE payload = payload",
            ),
        ] {
            let statements = glue.plan(sql).unwrap();
            assert_eq!(statements.len(), 1);
            let statement = &statements[0];
            validate_result(glue.execute_stmt(statement).unwrap(), operation, &value);
            c.bench_function(&format!("query_evaluation/{name}/{operation}"), |b| {
                b.iter(|| {
                    let result = glue.execute_stmt(black_box(statement)).unwrap();
                    drop(black_box(result));
                });
            });
        }
    }
}

criterion_group!(benches, bench_query_evaluation);
criterion_main!(benches);
