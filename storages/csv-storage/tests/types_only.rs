use {
    gluesql_core::prelude::{
        Glue, Payload,
        Value::{Date, Decimal, Null, Str, I64},
    },
    gluesql_csv_storage::CsvStorage,
    rust_decimal_macros::dec,
    std::collections::HashMap,
};

macro_rules! date {
    ($date: expr) => {
        Date($date.parse().unwrap())
    };
}

#[tokio::test]
async fn types_only() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue
        .execute("SELECT * FROM Grocery")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = [
        vec![
            ("Product Name", Str("Apple".to_owned())),
            ("Price", Decimal(dec!(1.99))),
            ("Quantity", I64(100)),
            ("Manufacturer", Str("Apple Inc.".to_owned())),
            ("Expiration Date", date!("2022-08-31")),
        ],
        vec![
            ("Product Name", Str("Banana".to_owned())),
            ("Price", Decimal(dec!(0.99))),
            ("Quantity", I64(50)),
            ("Manufacturer", Str("Chiquita Brands LLC".to_owned())),
            ("Expiration Date", date!("2022-09-01")),
        ],
        vec![
            ("Product Name", Str("Milk".to_owned())),
            ("Price", Decimal(dec!(2.49))),
            ("Manufacturer", Str("Dean Foods Company".to_owned())),
            ("Expiration Date", date!("2022-08-30")),
        ],
        vec![
            ("Product Name", Str("Bread".to_owned())),
            ("Price", Decimal(dec!(1.99))),
            ("Quantity", I64(30)),
            ("Manufacturer", Str("Sara Lee Corporation".to_owned())),
            ("Expiration Date", Null),
        ],
        vec![
            ("Product Name", Str("Eggs".to_owned())),
            ("Price", Decimal(dec!(2.99))),
            ("Quantity", I64(40)),
            ("Expiration Date", date!("2022-09-02")),
        ],
    ]
    .into_iter()
    .map(|row| {
        row.into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect::<HashMap<_, _>>()
    })
    .collect::<Vec<_>>();
    let expected = Payload::SelectMap(expected);
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            r#"
            SELECT "Product Name" AS name
            FROM Grocery
            WHERE "Expiration Date" IS NULL
            "#,
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = Payload::Select {
        labels: vec!["name".to_owned()],
        rows: vec![vec![Str("Bread".to_owned())]],
    };
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            r#"
            SELECT "Product Name" AS name
            FROM Grocery
            WHERE
                Price >= 2 AND
                "Expiration Date" < DATE '2022-09-02'
            "#,
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = Payload::Select {
        labels: vec!["name".to_owned()],
        rows: vec![vec![Str("Milk".to_owned())]],
    };
    assert_eq!(actual, expected);
}
