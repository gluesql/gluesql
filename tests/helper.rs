use gluesql::{execute, Payload, SledStorage, Store};
use nom_sql::parse_query;

pub struct Helper<T> {
    storage: Box<dyn Store<T>>,
}

impl Helper<u64> {
    pub fn new(path: &str) -> Self {
        let storage = Box::new(SledStorage::new(path.to_string()));

        Helper { storage }
    }

    pub fn run<U64>(&self, sql: &str) -> Result<Payload<u64>, ()> {
        let parsed = parse_query(sql).unwrap();
        println!("[Run] {}", parsed);

        execute(self.storage.as_ref(), &parsed)
    }

    pub fn run_and_print<U64>(&self, sql: &str) {
        let result = self.run::<u64>(sql);

        match result.unwrap() {
            Payload::Select(rows) => println!("[Ok ]\n{:#?}\n", rows),
            Payload::Insert(row) => println!("[Ok ]\n{:#?}\n", row),
            Payload::Delete(num) => println!("[Ok ] {} rows deleted.\n", num),
            Payload::Update(num) => println!("[Ok ] {} rows updated.\n", num),
            Payload::Create => println!("[Ok ] :)\n"),
        };
    }

    pub fn test_rows<U64>(&self, sql: &str, count: usize) {
        let result = self.run::<u64>(sql);

        match result.unwrap() {
            Payload::Select(rows) => assert_eq!(rows.len(), count),
            Payload::Delete(num) => assert_eq!(num, count),
            Payload::Update(num) => assert_eq!(num, count),
            _ => panic!("compare is only for Select, Delete and Update"),
        };
    }

    pub fn test_columns<U64>(&self, sql: &str, count: usize) {
        let result = self.run::<u64>(sql);

        match result.unwrap() {
            Payload::Select(rows) => {
                assert_eq!(rows.into_iter().nth(0).unwrap().items.len(), count)
            }
            _ => assert!(false),
        };
    }
}
