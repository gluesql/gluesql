use {
    async_trait::async_trait,
    gluesql_core::{
        ast::*,
        parse_sql::parse_expr,
        prelude::*,
        result::Result,
        store::{GStore, GStoreMut},
        translate::translate_expr,
    },
};

pub mod macros;

pub fn expr(sql: &str) -> Expr {
    let parsed = parse_expr(sql).unwrap();

    translate_expr(&parsed).unwrap()
}

pub fn test(found: Result<Payload>, expected: Result<Payload>) {
    let (found, expected): (Payload, Payload) = match (found, expected) {
        (Ok(a), Ok(b)) => (a, b),
        (a, b) => {
            assert_eq!(a, b);

            return;
        }
    };

    let (found, expected) = match (found, expected) {
        (
            Payload::Select {
                labels: found_labels,
                rows: a,
            },
            Payload::Select {
                labels: expected_labels,
                rows: b,
            },
        ) => {
            assert_eq!(expected_labels, found_labels);

            (a, b)
        }
        (a, b) => {
            assert_eq!(a, b);

            return;
        }
    };

    assert_eq!(
        found.len(),
        expected.len(),
        "\n[err: number of rows]\n   found: {:?}\nexpected: {:?}",
        found,
        expected
    );

    let rows = expected.into_iter().zip(found.into_iter()).enumerate();

    for (i, (found, expected)) in rows {
        assert_eq!(
            found.len(),
            expected.len(),
            "\n[err: size of row] row index: {}\n   found: {:?}\nexpected: {:?}",
            i,
            found,
            expected
        );

        expected
            .iter()
            .zip(found.iter())
            .for_each(|(found_val, expected_val)| {
                if matches!((found_val, expected_val), (&Value::Null, &Value::Null)) {
                    return;
                }

                assert_eq!(
                    found_val, expected_val,
                    "\n[err: value] row index: {}\n   found: {:?}\nexpected: {:?}",
                    i, found, expected
                );
            });
    }
}

pub async fn run<T: GStore + GStoreMut>(
    sql: &str,
    glue: &mut Glue<T>,
    indexes: Option<Vec<IndexItem>>,
) -> Result<Payload> {
    let storage = glue.storage.as_ref().unwrap();

    println!("[SQL] {}", sql);
    let parsed = parse(sql)?;
    let statement = translate(&parsed[0])?;
    let statement = plan(storage, statement).await?;

    test_indexes(&statement, indexes);

    glue.execute_stmt_async(&statement).await
}

pub fn test_indexes(statement: &Statement, indexes: Option<Vec<IndexItem>>) {
    if let Some(expected) = indexes {
        let found = find_indexes(statement);

        if expected.len() != found.len() {
            panic!(
                "num of indexes does not match: expected({}) != found({})",
                expected.len(),
                found.len(),
            );
        }

        for expected_index in expected {
            if !found
                .iter()
                .any(|found_index| *found_index == &expected_index)
            {
                panic!("index does not exist: {:#?}", expected_index)
            }
        }
    }
}

fn find_indexes(statement: &Statement) -> Vec<&IndexItem> {
    fn find_expr_indexes(expr: &Expr) -> Vec<&IndexItem> {
        match expr {
            Expr::Subquery(query)
            | Expr::Exists {
                subquery: query, ..
            }
            | Expr::InSubquery {
                subquery: query, ..
            } => find_query_indexes(query),
            _ => vec![],
        }
    }

    fn find_query_indexes(query: &Query) -> Vec<&IndexItem> {
        let select = match &query.body {
            SetExpr::Select(select) => select,
            _ => {
                return vec![];
            }
        };

        let selection_indexes = select
            .selection
            .as_ref()
            .map(find_expr_indexes)
            .unwrap_or_default();

        let table_indexes = match &select.from.relation {
            TableFactor::Table {
                index: Some(index), ..
            } => vec![index],
            _ => vec![],
        };

        [selection_indexes, table_indexes].concat()
    }

    match statement {
        Statement::Query(query) => find_query_indexes(query),
        _ => vec![],
    }
}

pub fn type_match(expected: &[DataType], found: Result<Payload>) {
    let rows = match found {
        Ok(Payload::Select {
            labels: _expected_labels,
            rows,
        }) => rows,
        _ => panic!("type match is only for Select"),
    };

    for (i, items) in rows.iter().enumerate() {
        assert_eq!(
            items.len(),
            expected.len(),
            "\n[err: size of row] row index: {}\n expected: {:?}\n found: {:?}",
            i,
            expected.len(),
            items.len()
        );

        items
            .iter()
            .zip(expected.iter())
            .for_each(|(value, data_type)| match value.validate_type(data_type) {
                Ok(_) => {}
                Err(_) => panic!(
                    "[err: type match failed]\n expected {:?}\n found {:?}\n",
                    data_type, value
                ),
            })
    }
}

/// If you want to make your custom storage and want to run integrate tests,
/// you should implement this `Tester` trait.
///
/// To see how to use it,
/// * [tests/memory_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/memory-storage/tests/memory_storage.rs)
/// * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/storages/sled-storage/tests/sled_storage.rs)
///
/// Actual test cases are in [test-suite/src/](https://github.com/gluesql/gluesql/blob/main/test-suite/src/),
/// not in `/tests/`.
#[async_trait(?Send)]
pub trait Tester<T: GStore + GStoreMut> {
    async fn new(namespace: &str) -> Self;

    fn get_glue(&mut self) -> &mut Glue<T>;
}

#[macro_export]
macro_rules! test_case {
    ($name: ident, $content: expr) => {
        pub async fn $name<T>(mut tester: impl $crate::Tester<T>)
        where
            T: gluesql_core::store::GStore + gluesql_core::store::GStoreMut,
        {
            let glue = tester.get_glue();

            #[allow(unused_macros)]
            macro_rules! get_glue {
                () => {
                    glue
                };
            }

            #[allow(unused_macros)]
            macro_rules! schema {
                ($table_name: literal) => {
                    glue.storage
                        .as_ref()
                        .expect("storage is empty")
                        .fetch_schema($table_name)
                        .await
                        .expect("error fetching schema")
                        .expect("table not found")
                };
            }

            #[allow(unused_macros)]
            macro_rules! expr {
                ($sql: literal) => {
                    $crate::expr($sql)
                };
            }

            #[allow(unused_macros)]
            macro_rules! run {
                ($sql: expr) => {
                    $crate::run($sql, glue, None).await.unwrap()
                };
            }

            #[allow(unused_macros)]
            macro_rules! run_err {
                ($sql: expr) => {
                    $crate::run($sql, glue, None).await.unwrap_err()
                };
            }

            #[allow(unused_macros)]
            macro_rules! count {
                ($count: expr, $sql: expr) => {
                    match $crate::run($sql, glue, None).await.unwrap() {
                        gluesql_core::prelude::Payload::Select { rows, .. } => {
                            assert_eq!($count, rows.len())
                        }
                        gluesql_core::prelude::Payload::Delete(num) => assert_eq!($count, num),
                        gluesql_core::prelude::Payload::Update(num) => assert_eq!($count, num),
                        _ => panic!("compare is only for Select, Delete and Update"),
                    };
                };
            }

            #[allow(unused_macros)]
            macro_rules! type_match {
                ($expected: expr, $sql: expr) => {
                    let found = run($sql, glue, None).await;

                    $crate::type_match($expected, found);
                };
            }

            #[allow(unused_macros)]
            macro_rules! test {
                (name: $test_name: literal, sql: $sql: expr, expected: $expected: expr) => {
                    let found = run($sql, glue, None).await;

                    $crate::test(found, $expected);
                };

                (sql: $sql: expr, expected: $expected: expr) => {
                    let found = run($sql, glue, None).await;

                    $crate::test(found, $expected);
                };

                ($sql: expr, $expected: expr) => {
                    let found = run($sql, glue, None).await;

                    $crate::test(found, $expected);
                };
            }

            #[allow(unused_macros)]
            macro_rules! test_idx {
                ($expected: expr, $indexes: expr, $sql: expr) => {
                    let found = run($sql, glue, Some($indexes)).await;

                    $crate::test($expected, found);
                };
            }

            $content.await
        }
    };
}
