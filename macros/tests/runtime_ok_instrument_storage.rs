use gluesql_macros::trace_storage;

type Result<T> = std::result::Result<T, &'static str>;
type Rows = Box<dyn Iterator<Item = Result<i32>>>;

trait ExternalStore {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>>;
    fn stream(&self) -> Result<Rows>;
}

struct Storage;

#[trace_storage(name = "inherent", skip(identity))]
impl Storage {
    fn scan_data(&self) -> Vec<i32> {
        vec![1, 2]
    }

    fn identity<T>(value: T) -> T {
        value
    }

    fn row_value(rows: i32) -> i32 {
        rows
    }
}

#[cfg_attr(all(), trace_storage(name = "external", iterators(stream)))]
impl ExternalStore for Storage {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>> {
        Ok(rows.into_iter().filter(|value| *value == key).collect())
    }

    fn stream(&self) -> Result<Rows> {
        Ok(Box::new([Ok(1), Err("broken row"), Ok(2)].into_iter()))
    }
}

#[test]
fn instruments_external_trait_without_changing_calls() {
    struct Opaque;
    let storage = Storage;

    assert_eq!(storage.scan_data(), vec![1, 2]);
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

#[cfg_attr(any(), trace_storage(name = "off", iterators(stream)))]
impl ExternalStore for UntracedStorage {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>> {
        Storage.lookup(key, rows)
    }

    fn stream(&self) -> Result<Rows> {
        Ok(Box::new([Ok(4)].into_iter()))
    }
}
