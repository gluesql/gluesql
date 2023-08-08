use {
    gluesql_core::prelude::{
        Glue,
        Value::{self, Str},
    },
    gluesql_csv_storage::CsvStorage,
    serde_json::json,
    std::fs,
    test_suite::*,
};

#[tokio::test]
async fn schemaless_without_types() {
    let path = "./tests/samples/";
    let storage = CsvStorage::new(path).unwrap();
    let mut glue = Glue::new(storage);

    let actual = glue
        .execute("SELECT * FROM Book")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_map![
        json!({
            "Title": "To Kill a Mockingbird",
            "Author": "Harper Lee",
            "Genre": "",
            "Year": "1960",
            "Publisher": "J. B. Lippincott & Co.",
            "Price": ""
        }),
        json!({
            "Title": "The Great Gatsby",
            "Author": "F. Scott Fitzgerald",
            "Genre": "Classic",
            "Year": "1925",
            "Publisher": "",
            "Price": "9.99"
        }),
        json!({
            "Title": "1984",
            "Author": "George Orwell",
            "Genre": "Dystopian",
            "Year": "",
            "Publisher": "Secker & Warburg",
            "Price": "7.99"
        }),
        json!({
            "Title": "The Catcher in the Rye",
            "Author": "J. D. Salinger",
            "Genre": "Coming-of-age",
            "Year": "1951",
            "Publisher": "Little, Brown and Company",
            "Price": "6.99"
        }),
        json!({
            "Title": "Pride and Prejudice",
            "Author": "Jane Austen",
            "Genre": "Romance",
            "Year": "1813",
            "Publisher": "",
            "Price": ""
        })
    ];
    assert_eq!(actual, expected);

    let actual = glue
        .execute(
            "
            SELECT Title
            FROM Book
            WHERE
                CASE LENGTH(Price) 
                    WHEN 0 THEN FALSE
                    ELSE CAST(Price AS DECIMAL) > 6.99
                END
            ",
        )
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select!(
        Title
        Str;
        "The Great Gatsby".to_owned();
        "1984".to_owned()
    );
    assert_eq!(actual, expected);

    glue.execute(
        r#"
        INSERT INTO Book
        VALUES (
            '{ "Title": "New Book Temporary", "Price": "100" }'
        )
        "#,
    )
    .await
    .unwrap();

    let actual = glue
        .execute("SELECT * FROM Book WHERE Price = '100'")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_map![json!({
        "Title": "New Book Temporary",
        "Price": "100"
    })];
    assert_eq!(actual, expected);

    glue.execute(
        r#"
        UPDATE Book SET Year = '1925' WHERE Title = 'New Book Temporary'
        "#,
    )
    .await
    .unwrap();

    let actual = glue
        .execute("SELECT * FROM Book WHERE Year = '1925'")
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let expected = select_map![
        json!({
            "Title": "The Great Gatsby",
            "Author": "F. Scott Fitzgerald",
            "Genre": "Classic",
            "Year": "1925",
            "Publisher": "",
            "Price": "9.99"
        }),
        json!({
            "Title": "New Book Temporary",
            "Year": "1925",
            "Price": "100"
        })
    ];
    assert_eq!(actual, expected);

    glue.execute("DELETE FROM Book WHERE Title = 'New Book Temporary'")
        .await
        .unwrap();

    fs::remove_file(format!("{path}Book.types.csv")).unwrap_or(());
}
