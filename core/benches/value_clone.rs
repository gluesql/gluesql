use {
    criterion::{BatchSize, Criterion, criterion_group, criterion_main},
    gluesql_core::data::Value,
    std::{collections::BTreeMap, hint::black_box, rc::Rc},
};

fn inputs() -> Vec<(String, Value)> {
    let mut inputs = vec![("i64/42".to_owned(), Value::I64(42))];

    for bytes in [16, 256, 4_096, 65_536] {
        let text = "x".repeat(bytes);
        assert_eq!(text.len(), bytes);
        inputs.push((format!("string/{bytes}_bytes"), Value::Str(text)));
    }

    for entries in [8, 128] {
        for bytes in [16, 256] {
            let map: BTreeMap<_, _> = (0..entries)
                .map(|index| (format!("key_{index:04}"), Value::Str("x".repeat(bytes))))
                .collect();
            assert_eq!(map.len(), entries);
            for (key, value) in &map {
                assert_eq!(key.len(), 8);
                assert!(matches!(value, Value::Str(text) if text.len() == bytes));
            }
            inputs.push((
                format!("map/{entries}_entries/{bytes}_bytes_per_value"),
                Value::Map(map),
            ));
        }
    }

    inputs
}

fn bench_value_clone(c: &mut Criterion) {
    for (name, value) in inputs() {
        assert_eq!(value.clone(), value);
        let shared = Rc::new(value.clone());
        let cloned = Rc::clone(&shared);
        assert!(Rc::ptr_eq(&shared, &cloned));
        assert_eq!(Rc::strong_count(&shared), 2);
        drop(cloned);
        assert_eq!(Rc::strong_count(&shared), 1);

        c.bench_function(&format!("value_clone_drop/{name}"), |b| {
            b.iter(|| drop(black_box(black_box(&value).clone())));
        });

        c.bench_function(&format!("rc_clone_drop/{name}"), |b| {
            b.iter(|| drop(black_box(Rc::clone(black_box(&shared)))));
        });

        c.bench_function(&format!("rc_new/{name}"), |b| {
            b.iter_batched(
                || black_box(&value).clone(),
                |input| black_box(Rc::new(black_box(input))),
                BatchSize::NumIterations(64),
            );
        });
    }
}

criterion_group!(benches, bench_value_clone);
criterion_main!(benches);
