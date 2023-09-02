use {
    gluesql_core::prelude::{
        Glue,
        Value::{Str, I64},
    },
    gluesql_csv_storage::CsvStorage,
    test_suite::*,
};

#[tokio::test]
async fn basic() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue
        .execute("SELECT * FROM Employee")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select! {
        Name                | Age            | Gender             | Occupation
        Str                 | Str            | Str                | Str;
        "John".to_owned()     "25".to_owned()  "Male".to_owned()    "Engineer".to_owned();
        "Sarah".to_owned()    "30".to_owned()  "Female".to_owned()  "Doctor".to_owned();
        "Michael".to_owned()  "40".to_owned()  "Male".to_owned()    "Lawyer".to_owned();
        "Emily".to_owned()    "28".to_owned()  "Female".to_owned()  "Teacher".to_owned();
        "David".to_owned()    "35".to_owned()  "Male".to_owned()    "Programmer".to_owned()
    };
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            "
            SELECT
                Name,
                CAST(Age AS INTEGER) AS Age
            FROM Employee
         ",
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select! {
        Name                | Age
        Str                 | I64;
        "John".to_owned()     25;
        "Sarah".to_owned()    30;
        "Michael".to_owned()  40;
        "Emily".to_owned()    28;
        "David".to_owned()    35
    };
    assert_eq!(actual, expected);
}
