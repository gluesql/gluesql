use {
    crate::*,
    gluesql_core::{ast::DataType, error::ExecuteError, executor::Payload},
};

test_case!(show_columns, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE mytable (
            id8 INT8,
            id INTEGER,
            rate FLOAT,
            dec  decimal,
            flag BOOLEAN,
            text TEXT,
            DOB  Date,
            Tm   Time,
            ival Interval,
            tstamp Timestamp,
            uid    Uuid,
            hash   Map,
            glist  List
        );
    ",
    )
    .await;

    g.test(
        r#"Show columns from mytable"#,
        Ok(Payload::ShowColumns(vec![
            ("id8".to_owned(), DataType::Int8),
            ("id".to_owned(), DataType::Int),
            ("rate".to_owned(), DataType::Float),
            ("dec".to_owned(), DataType::Decimal),
            ("flag".to_owned(), DataType::Boolean),
            ("text".to_owned(), DataType::Text),
            ("DOB".to_owned(), DataType::Date),
            ("Tm".to_owned(), DataType::Time),
            ("ival".to_owned(), DataType::Interval),
            ("tstamp".to_owned(), DataType::Timestamp),
            ("uid".to_owned(), DataType::Uuid),
            ("hash".to_owned(), DataType::Map),
            ("glist".to_owned(), DataType::List),
        ])),
    )
    .await;

    g.test(
        r#"Show columns from mytable1"#,
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into()),
    )
    .await;
});
