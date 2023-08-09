use {
    async_trait::async_trait,
    gluesql_core::{
        ast::*,
        parse_sql::parse_expr,
        prelude::*,
        store::{GStore, GStoreMut},
        translate::translate_expr,
    },
    pretty_assertions::assert_eq,
};

pub mod macros;

pub fn expr(sql: &str) -> Expr {
    let parsed = parse_expr(sql).unwrap();

    translate_expr(&parsed).unwrap()
}

pub async fn run<T: GStore + GStoreMut>(
    sql: &str,
    glue: &mut Glue<T>,
    indexes: Option<Vec<IndexItem>>,
) -> Result<Payload> {
    println!("[SQL] {}", sql);
    let parsed = parse(sql)?;
    let statement = translate(&parsed[0])?;
    let statement = plan(&glue.storage, statement).await?;

    test_indexes(&statement, indexes);

    glue.execute_stmt(&statement).await
}

pub fn test_indexes(statement: &Statement, indexes: Option<Vec<IndexItem>>) {
    if let Some(expected) = indexes {
        let found = find_indexes(statement);

        if expected.len() != found.len() {
            panic!(
                "num of indexes does not match: found({}) != expected({})",
                found.len(),
                expected.len(),
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
            "\n[err: size of row] row index: {}\n found: {:?}\n expected: {:?}",
            i,
            items.len(),
            expected.len()
        );

        items
            .iter()
            .zip(expected.iter())
            .for_each(|(value, data_type)| match value.validate_type(data_type) {
                Ok(_) => {}
                Err(_) => panic!(
                    "[err: type match failed]\n found {:?}\n expected {:?}\n",
                    value, data_type
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

    async fn run(&mut self, sql: &str) -> Result<Payload> {
        let glue = self.get_glue();

        println!("[RUN] {}", sql);
        let parsed = parse(sql)?;
        let statement = translate(&parsed[0])?;
        let statement = plan(&glue.storage, statement).await?;

        glue.execute_stmt(&statement).await
    }

    async fn count(&mut self, sql: &str, expected: usize) {
        let actual = match self.run(sql).await.unwrap() {
            Payload::Select { rows, .. } => rows.len(),
            Payload::Delete(num) | Payload::Update(num) => num,
            _ => panic!("compare is only for Select, Delete and Update"),
        };

        assert_eq!(actual, expected, "[COUNT] {sql}");
    }

    async fn type_match(&mut self, sql: &str, expected: &[DataType]) {
        let actual = self.run(sql).await.unwrap();

        type_match(expected, Ok(actual));
    }

    async fn test(&mut self, sql: &str, expected: Result<Payload>) {
        let actual = self.run(sql).await;

        assert_eq!(actual, expected, "[TEST] {sql}");
    }

    async fn named_test(&mut self, name: &str, sql: &str, expected: Result<Payload>) {
        let actual = self.run(sql).await;

        assert_eq!(actual, expected, "[TEST] {name}");
    }
}

#[macro_export]
macro_rules! test_case {
    ($name: ident, $content: expr) => {
        pub async fn $name<T>(mut tester: impl $crate::Tester<T>)
        where
            T: gluesql_core::store::GStore + gluesql_core::store::GStoreMut,
        {
            #[allow(unused_variables)]
            let glue = tester.get_glue();

            #[allow(unused_macros)]
            macro_rules! get_glue {
                () => {
                    glue
                };
            }

            #[allow(unused_macros)]
            macro_rules! get_tester {
                () => {
                    &mut tester
                };
            }

            #[allow(unused_macros)]
            macro_rules! schema {
                ($table_name: literal) => {
                    glue.storage
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

                    assert_eq!(found, $expected, $test_name);
                };

                (sql: $sql: expr, expected: $expected: expr) => {
                    let found = run($sql, glue, None).await;

                    assert_eq!(found, $expected);
                };

                ($sql: expr, $expected: expr) => {
                    let found = run($sql, glue, None).await;

                    assert_eq!(found, $expected);
                };
            }

            #[allow(unused_macros)]
            macro_rules! test_idx {
                ($expected: expr, $indexes: expr, $sql: expr) => {
                    let found = run($sql, glue, Some($indexes)).await;

                    assert_eq!(found, $expected);
                };
            }

            $content.await
        }
    };
}
