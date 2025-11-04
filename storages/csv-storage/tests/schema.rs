use {
    gluesql_core::prelude::{
        Glue,
        Value::{I64, Str},
    },
    gluesql_csv_storage::CsvStorage,
    test_suite::*,
};

#[tokio::test]
async fn schema() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue
        .execute("SELECT * FROM City")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select! {
        Country                    | City                       | Population
        Str                        | Str                        | I64;
        "South Korea".to_owned()     "Seoul".to_owned()           9_736_962;
        "Japan".to_owned()           "Tokyo".to_owned()           13_515_271;
        "China".to_owned()           "Shanghai".to_owned()        24_281_000;
        "United States".to_owned()   "New York City".to_owned()   8_336_817;
        "Italy".to_owned()           "Milan".to_owned()           2_837_332
    };
    assert_eq!(actual, expected);

    let actual = glue
        .execute("SELECT * FROM City WHERE Population < 10000000")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select! {
        Country                    | City                       | Population
        Str                        | Str                        | I64;
        "South Korea".to_owned()     "Seoul".to_owned()           9_736_962;
        "United States".to_owned()   "New York City".to_owned()   8_336_817;
        "Italy".to_owned()           "Milan".to_owned()           2_837_332
    };
    assert_eq!(actual, expected);
}
