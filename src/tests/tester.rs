use async_trait::async_trait;
use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use crate::executor::{execute, Payload};
use crate::parse_sql::parse;
use crate::result::Result;
use crate::store::{AlterTable, Store, StoreMut};

pub async fn run<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable>(
    cell: Rc<RefCell<Option<U>>>,
    sql: &str,
) -> Result<Payload> {
    println!("[Run] {}", sql);

    let mut storage = cell.replace(None).unwrap();

    let query = &parse(sql).unwrap()[0];

    let result = match execute(storage, query).await {
        Ok((s, payload)) => {
            storage = s;

            Ok(payload)
        }
        Err((s, error)) => {
            storage = s;

            Err(error)
        }
    };

    cell.replace(Some(storage));

    result
}

/// If you want to make your custom storage and want to run integrate tests,
/// you should implement this `Tester` trait.
///
/// To see how to use it,
/// * [tests/sled_storage.rs](https://github.com/gluesql/gluesql/blob/main/tests/sled_storage.rs)
///
/// Actual test cases are in [/src/tests/](https://github.com/gluesql/gluesql/blob/main/src/tests/),
/// not in `/tests/`.
#[async_trait]
pub trait Tester<T: 'static + Debug, U: Store<T> + StoreMut<T> + AlterTable> {
    fn new(namespace: &str) -> Self;

    fn get_cell(&mut self) -> Rc<RefCell<Option<U>>>;
}

#[macro_export]
macro_rules! build_suite {
    ($suite: ident; $($module: ident),+ $(,($inline_module_name: ident, $inline_module: path))*) => {
        $(pub mod $module;)+
        #[macro_export]
        macro_rules! $suite {
            ($test: meta, $storage: ident) => {
                macro_rules! wrap {
                    ($name: ident) => {
                        wrap!($name, $suite::$name::test);
                    };
                    ($name: ident, $path: path) => {
                        #[$test]
                        // Once possible concat_idents!($ suite, $ name) would be nice (rustlang/rust#12249, rustlang/rust#29599)
                        async fn $name() {
                            let path = stringify!($name);
                            let storage = $storage::new(path);

                            $path(storage).await;
                        }
                    };
                }
                $(wrap!($module);)+
                $(wrap!($inline_module_name, $inline_module);)*
            };
        }
    };
}

#[macro_export]
macro_rules! test_case {
    ($content: expr) => {
        test_case!(test, $content);
    };
    ($name: ident, $content: expr) => {
        #[allow(dead_code)]
        pub async fn $name<T, U>(mut tester: impl tests::Tester<T, U>)
        where
            T: 'static + std::fmt::Debug,
            U: Store<T> + StoreMut<T> + AlterTable,
        {
            use std::rc::Rc;

            let cell = tester.get_cell();

            #[allow(unused_macros)]
            macro_rules! run {
                ($sql: expr) => {
                    tests::run(Rc::clone(&cell), $sql).await.unwrap()
                };
            }

            #[allow(unused_macros)]
            macro_rules! count {
                ($count: expr, $sql: expr) => {
                    match tests::run(Rc::clone(&cell), $sql).await.unwrap() {
                        Payload::Select { rows, .. } => assert_eq!($count, rows.len()),
                        Payload::Delete(num) => assert_eq!($count, num),
                        Payload::Update(num) => assert_eq!($count, num),
                        _ => panic!("compare is only for Select, Delete and Update"),
                    };
                };
            }

            #[allow(unused_macros)]
            macro_rules! test {
                ($expected: expr, $sql: expr) => {
                    let found = tests::run(Rc::clone(&cell), $sql).await;

                    test($expected, found);
                };
            }

            #[allow(unused)]
            fn test(expected: Result<Payload>, found: Result<Payload>) {
                let (expected, found): (Payload, Payload) = match (expected, found) {
                    (Ok(a), Ok(b)) => (a, b),
                    (a, b) => {
                        assert_eq!(a, b);

                        return;
                    }
                };

                let (expected, found) = match (expected, found) {
                    (
                        Payload::Select {
                            labels: expected_labels,
                            rows: a,
                        },
                        Payload::Select {
                            labels: found_labels,
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
                    expected.len(),
                    found.len(),
                    "\n[err: number of rows]\nexpected: {:?}\n   found: {:?}",
                    expected,
                    found
                );

                let rows = expected.into_iter().zip(found.into_iter()).enumerate();

                for (i, (expected, found)) in rows.into_iter() {
                    let Row(expected) = expected;
                    let Row(found) = found;

                    assert_eq!(
                        expected.len(),
                        found.len(),
                        "\n[err: size of row] row index: {}\nexpected: {:?}\n   found: {:?}",
                        i,
                        expected,
                        found
                    );

                    expected
                        .iter()
                        .zip(found.iter())
                        .for_each(|(expected_val, found_val)| {
                            if matches!((expected_val, found_val), (&Value::Null, &Value::Null)) {
                                return;
                            }

                            assert_eq!(
                                expected_val, found_val,
                                "\n[err: value] row index: {}\nexpected: {:?}\n   found: {:?}",
                                i, expected, found
                            );
                        });
                }
            }

            $content.await
        }
    };
}
