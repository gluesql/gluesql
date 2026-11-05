use gluesql_core::observe;

type Result<T> = std::result::Result<T, &'static str>;
type Rows = Box<dyn Iterator<Item = Result<i32>>>;

trait ExternalStore {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>>;
    fn stream(&self) -> Result<Rows>;
}

struct Storage;

impl Storage {
    #[observe]
    fn coerced_stream(empty: bool) -> Result<Rows> {
        if empty {
            return Ok(Box::new(std::iter::empty()));
        }
        Ok(Box::new(std::iter::once(Ok(1))))
    }

    #[observe]
    async fn async_stream(empty: bool) -> Result<Rows> {
        std::future::ready(()).await;
        if empty {
            return Ok(Box::new(std::iter::empty()));
        }
        Ok(Box::new(std::iter::once(Ok(2))))
    }

    #[observe]
    fn scan_data() -> Vec<i32> {
        vec![1, 2]
    }

    #[observe]
    fn identity<T>(value: T) -> T {
        value
    }

    #[observe]
    fn row_value(rows: i32) -> i32 {
        rows
    }
}

#[test]
fn preserves_iterator_return_coercions() {
    assert_eq!(
        Storage::coerced_stream(false).unwrap().collect::<Vec<_>>(),
        vec![Ok(1)]
    );
    assert!(Storage::coerced_stream(true).unwrap().next().is_none());
}

#[test]
fn preserves_async_iterator_bodies() {
    for empty in [false, true] {
        let mut future = Box::pin(Storage::async_stream(empty));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let std::task::Poll::Ready(rows) = std::future::Future::poll(future.as_mut(), &mut context)
        else {
            panic!("ready future should complete on its first poll");
        };
        assert_eq!(
            rows.unwrap().collect::<Vec<_>>(),
            if empty { vec![] } else { vec![Ok(2)] }
        );
    }
}

struct MutableStorage(Vec<u8>);

impl MutableStorage {
    #[observe]
    fn stream(&mut self) -> Result<Box<dyn Iterator<Item = Result<&mut u8>> + '_>> {
        Ok(Box::new(self.0.iter_mut().map(Ok)))
    }
}

#[test]
fn preserves_mutable_iterator_borrows() {
    let mut storage = MutableStorage(vec![1, 2]);
    for row in storage.stream().unwrap() {
        *row.unwrap() += 1;
    }
    assert_eq!(storage.0, vec![2, 3]);
}

impl ExternalStore for Storage {
    #[observe]
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>> {
        Ok(rows.into_iter().filter(|value| *value == key).collect())
    }

    #[observe]
    fn stream(&self) -> Result<Rows> {
        Ok(Box::new([Ok(1), Err("broken row"), Ok(2)].into_iter()))
    }
}

#[test]
fn instruments_external_trait_without_changing_calls() {
    struct Opaque;
    let storage = Storage;

    assert_eq!(Storage::scan_data(), vec![1, 2]);
    let _: Opaque = Storage::identity(Opaque);
    assert_eq!(Storage::row_value(3), 3);
    assert_eq!(
        UntracedStorage.stream().unwrap().collect::<Vec<_>>(),
        vec![Ok(4)]
    );

    assert_eq!(storage.lookup(2, vec![1, 2, 3]), Ok(vec![2]));
    assert_eq!(
        storage.stream().unwrap().collect::<Vec<_>>(),
        vec![Ok(1), Err("broken row"), Ok(2)]
    );
}

struct UntracedStorage;

impl ExternalStore for UntracedStorage {
    #[cfg_attr(any(), observe)]
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>> {
        Storage.lookup(key, rows)
    }

    #[cfg_attr(any(), observe)]
    fn stream(&self) -> Result<Rows> {
        Ok(Box::new([Ok(4)].into_iter()))
    }
}
