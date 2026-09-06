use gluesql_macros::trace_storage;

type Result<T> = std::result::Result<T, &'static str>;
type Rows = Box<dyn Iterator<Item = Result<i32>>>;

trait ExternalStore {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>>;
    fn stream(&self) -> Result<Rows>;
}

struct Storage;

#[trace_storage(name = "external")]
impl ExternalStore for Storage {
    fn lookup(&self, key: i32, rows: Vec<i32>) -> Result<Vec<i32>> {
        Ok(rows.into_iter().filter(|value| *value == key).collect())
    }

    #[trace_iterator]
    fn stream(&self) -> Result<Rows> {
        Ok(Box::new([Ok(1), Err("broken row"), Ok(2)].into_iter()))
    }
}

#[test]
fn instruments_external_trait_without_changing_calls() {
    let storage = Storage;

    assert_eq!(storage.lookup(2, vec![1, 2, 3]), Ok(vec![2]));
    assert_eq!(
        storage.stream().unwrap().collect::<Vec<_>>(),
        vec![Ok(1), Err("broken row"), Ok(2)]
    );
}
