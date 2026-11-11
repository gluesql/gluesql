use {
    crate::*,
    gluesql_core::{
        data::{
            Key, Schema,
            Value::{I64, Str},
        },
        error::Result,
        store::ScanColumns,
    },
    std::collections::BTreeMap,
};

test_case!(scan_data, {
    let storage = &mut get_glue!().storage;
    let schema = Schema::from_ddl(
        "CREATE TABLE Item (id INTEGER PRIMARY KEY, name TEXT, payload TEXT, price INTEGER)",
    )
    .unwrap();

    let transaction_started = storage.begin(true).unwrap();
    storage.insert_schema(&schema).unwrap();
    storage
        .insert_data(
            "Item",
            vec![
                (
                    Key::I64(1),
                    row!(I64 Str Str I64; 1 "Apple".to_owned() "red".to_owned() 30),
                ),
                (
                    Key::I64(2),
                    row!(I64 Str Str I64; 2 "Pear".to_owned() "green".to_owned() 20),
                ),
            ],
        )
        .unwrap();
    if transaction_started {
        storage.commit().unwrap();
    }

    let scan = |columns: ScanColumns<'_>| {
        storage
            .scan_data("Item", columns)
            .unwrap()
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap()
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };

    let (all_keys, actual) = scan(ScanColumns::All);
    let expected = vec![
        row!(I64 Str Str I64; 1 "Apple".to_owned() "red".to_owned() 30),
        row!(I64 Str Str I64; 2 "Pear".to_owned() "green".to_owned() 20),
    ];
    assert_eq!(actual, expected, "all columns");

    let columns = [("name".to_owned(), 1), ("price".to_owned(), 3)];
    let (selected_keys, actual) = scan(ScanColumns::Selected(&columns));
    let expected = vec![
        row!(Str I64; "Apple".to_owned() 30),
        row!(Str I64; "Pear".to_owned() 20),
    ];
    assert_eq!(actual, expected, "nonadjacent columns in schema order");
    assert_eq!(selected_keys, all_keys, "projection preserves keys");

    let (empty_keys, actual) = scan(ScanColumns::Selected(&[]));
    let expected = vec![vec![], vec![]];
    assert_eq!(actual, expected, "empty values preserve the row count");
    assert_eq!(empty_keys, all_keys, "empty values preserve keys");
});
