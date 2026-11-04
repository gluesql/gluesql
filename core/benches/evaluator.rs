use {
    criterion::{Criterion, criterion_group, criterion_main},
    gluesql_core::{
        ast::BinaryOperator,
        data::{Row, Value},
        executor::{EvaluateError, RowContext, evaluate_stateless},
        plan::ExprPlan,
    },
    std::{borrow::Cow, collections::BTreeMap, hint::black_box},
};

fn context(row: &Row) -> RowContext<'_> {
    RowContext::new("bench", Cow::Borrowed(row), None)
}

fn identifier(qualified: bool) -> ExprPlan {
    if qualified {
        ExprPlan::CompoundIdentifier {
            alias: "bench".to_owned(),
            ident: "payload".to_owned(),
        }
    } else {
        ExprPlan::Identifier("payload".to_owned())
    }
}

fn inputs() -> Vec<(&'static str, Value)> {
    vec![
        ("i64", Value::I64(42)),
        ("string_256", Value::Str("x".repeat(256))),
        ("string_4096", Value::Str("x".repeat(4_096))),
        (
            "map_8x256",
            Value::Map(
                (0..8)
                    .map(|index| (format!("key_{index:04}"), Value::Str("x".repeat(256))))
                    .collect::<BTreeMap<_, _>>(),
            ),
        ),
    ]
}

fn validate_context_errors() {
    let row = Row {
        columns: vec!["payload".to_owned()].into(),
        values: vec![Value::Null],
    };
    for qualified in [false, true] {
        let expr = identifier(qualified);
        let result = evaluate_stateless(Some(context(&row)), &expr).unwrap();
        assert_eq!(Value::try_from(result).unwrap(), Value::Null);
    }

    let missing = ExprPlan::Identifier("missing".to_owned());
    assert_eq!(
        evaluate_stateless(Some(context(&row)), &missing).unwrap_err(),
        EvaluateError::IdentifierNotFound("missing".to_owned()).into(),
    );
    for (alias, ident) in [("missing", "payload"), ("bench", "missing")] {
        let missing = ExprPlan::CompoundIdentifier {
            alias: alias.to_owned(),
            ident: ident.to_owned(),
        };
        assert_eq!(
            evaluate_stateless(Some(context(&row)), &missing).unwrap_err(),
            EvaluateError::CompoundIdentifierNotFound {
                table_alias: alias.to_owned(),
                column_name: ident.to_owned(),
            }
            .into(),
        );
    }
}

fn bench_evaluator(c: &mut Criterion) {
    validate_context_errors();
    for (name, value) in inputs() {
        let row = Row {
            columns: vec!["payload".to_owned()].into(),
            values: vec![value],
        };
        for (qualification, qualified) in [("plain", false), ("qualified", true)] {
            let expr = identifier(qualified);
            let equality = ExprPlan::BinaryOp {
                left: Box::new(expr.clone()),
                op: BinaryOperator::Eq,
                right: Box::new(expr.clone()),
            };
            let evaluated = evaluate_stateless(Some(context(&row)), &expr).unwrap();
            assert_eq!(Value::try_from(evaluated).unwrap(), row.values[0]);
            let evaluated = evaluate_stateless(Some(context(&row)), &equality).unwrap();
            assert_eq!(Value::try_from(evaluated).unwrap(), Value::Bool(true));

            c.bench_function(
                &format!("evaluator/{name}/{qualification}/evaluated_drop"),
                |b| {
                    b.iter(|| {
                        let evaluated =
                            evaluate_stateless(Some(context(black_box(&row))), black_box(&expr))
                                .unwrap();
                        drop(black_box(evaluated));
                    });
                },
            );
            c.bench_function(
                &format!("evaluator/{name}/{qualification}/value_drop"),
                |b| {
                    b.iter(|| {
                        let evaluated =
                            evaluate_stateless(Some(context(black_box(&row))), black_box(&expr))
                                .unwrap();
                        drop(black_box(Value::try_from(evaluated).unwrap()));
                    });
                },
            );
            c.bench_function(
                &format!("evaluator/{name}/{qualification}/equality_drop"),
                |b| {
                    b.iter(|| {
                        let evaluated = evaluate_stateless(
                            Some(context(black_box(&row))),
                            black_box(&equality),
                        )
                        .unwrap();
                        drop(black_box(evaluated));
                    });
                },
            );
        }
    }
}

criterion_group!(benches, bench_evaluator);
criterion_main!(benches);
