use {
    gluesql_core::{
        error::FetchError,
        prelude::{
            Glue,
            Value::{self, Null, Str, I64},
        },
    },
    gluesql_csv_storage::CsvStorage,
    serde_json::json,
    test_suite::*,
};

#[tokio::test]
async fn schemaless() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue
        .execute("SELECT * FROM Student")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_map![
        json!({
            "Name": "John",
            "Gender": "Male",
            "Age": 18,
            "Grade": "A"
        }),
        json!({
            "Name": "Jane",
            "Gender": "Female",
            "Age": 17,
            "Grade": "B"
        }),
        json!({
            "Name": "Bob",
            "Grade": "C"
        }),
        json!({
            "Name": "Alice",
            "Gender": "Female",
            "Age": 18
        }),
        json!({
            "Name": "Mike",
            "Gender": "Male",
            "Grade": "B"
        }),
        json!({
            "Name": "Lisa",
            "Age": 18,
            "Grade": "A"
        })
    ];
    assert_eq!(actual, expected);

    let actual = glue
        .execute("SELECT Name, Gender, Age, Grade FROM Student")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_with_null! {
        Name                    | Gender                   | Age     | Grade;
        Str("John".to_owned())    Str("Male".to_owned())     I64(18)   Str("A".to_owned());
        Str("Jane".to_owned())    Str("Female".to_owned())   I64(17)   Str("B".to_owned());
        Str("Bob".to_owned())     Null                       Null      Str("C".to_owned());
        Str("Alice".to_owned())   Str("Female".to_owned())   I64(18)   Null;
        Str("Mike".to_owned())    Str("Male".to_owned())     Null      Str("B".to_owned());
        Str("Lisa".to_owned())    Null                       I64(18)   Str("A".to_owned())
    };
    assert_eq!(actual, expected);

    let actual = glue
        .execute("SELECT Name FROM Student WHERE Age < 18")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select!(Name Str; "Jane".to_owned());
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn schemaless_create_and_drop_table() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    glue.execute("CREATE TABLE Foo").await.unwrap();
    glue.execute(r#"INSERT INTO Foo VALUES ('{ "a": 1 }')"#)
        .await
        .unwrap();

    let actual = glue
        .execute("SELECT * FROM Foo")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_map![json!({ "a": 1 })];
    assert_eq!(actual, expected);

    glue.execute("DROP TABLE Foo").await.unwrap();

    let actual = glue.execute("SELECT * FROM Foo").await;
    let expected = Err(FetchError::TableNotFound("Foo".to_owned()).into());
    assert_eq!(actual, expected);
}
