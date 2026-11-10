use {
    crate::*,
    gluesql_core::{
        data::{
            Key, Schema,
            Value::{I64, Str},
        },
        error::Result,
        store::ScanColumn,
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

    let scan = |columns: Option<&[ScanColumn]>| {
        storage
            .scan_data("Item", columns)
            .unwrap()
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap()
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>()
    };

    let (all_keys, actual) = scan(None);
    let expected = vec![
        row!(I64 Str Str I64; 1 "Apple".to_owned() "red".to_owned() 30),
        row!(I64 Str Str I64; 2 "Pear".to_owned() "green".to_owned() 20),
    ];
    assert_eq!(actual, expected, "all columns");

    let columns = [("price".to_owned(), 3), ("name".to_owned(), 1)];
    let (selected_keys, actual) = scan(Some(&columns));
    let expected = vec![
        row!(I64 Str; 30 "Apple".to_owned()),
        row!(I64 Str; 20 "Pear".to_owned()),
    ];
    assert_eq!(actual, expected, "nonadjacent columns in request order");
    assert_eq!(selected_keys, all_keys, "projection preserves keys");

    let (empty_keys, actual) = scan(Some(&[]));
    let expected = vec![vec![], vec![]];
    assert_eq!(actual, expected, "empty values preserve the row count");
    assert_eq!(empty_keys, all_keys, "empty values preserve keys");
});
