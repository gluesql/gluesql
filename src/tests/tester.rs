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
macro_rules! test_case {
    ($name: ident, $content: expr) => {
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

                    assert_eq!($expected, found);
                };
            }

            $content.await
        }
    };
}
