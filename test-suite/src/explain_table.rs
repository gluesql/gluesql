use {
    crate::*,
    gluesql_core::{
        ast::DataType,
        executor::{ExecuteError, Payload},
    },
};

test_case!(explain_table, async move {
    run!(
        "
        CREATE TABLE mytable (
            id8 INT8 PRIMARY KEY,
            id INTEGER UNIQUE,
            rate FLOAT NOT NULL,
            dec  decimal NOT NULL,
            flag BOOLEAN DEFAULT 0,
            text TEXT NOT NULL,
            DOB  Date NOT NULL,
            Tm   Time NOT NULL,
            ival Interval NOT NULL,
            tstamp Timestamp NOT NULL,
            uid    Uuid NOT NULL,
            hash   Map,
            glist  List NOT NULL,
        );
    "
    );

    test!(
        r#"EXPLAIN mytable"#,
        Ok(Payload::ExplainTable(vec![
            (
                "id8".to_owned(),
                DataType::Int8,
                false,
                "PRIMARY KEY".to_owned(),
                "".to_owned()
            ),
            (
                "id".to_owned(),
                DataType::Int,
                true,
                "UNIQUE".to_owned(),
                "".to_owned()
            ),
            (
                "rate".to_owned(),
                DataType::Float,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "dec".to_owned(),
                DataType::Decimal,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "flag".to_owned(),
                DataType::Boolean,
                true,
                "".to_owned(),
                "0".to_owned()
            ),
            (
                "text".to_owned(),
                DataType::Text,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "DOB".to_owned(),
                DataType::Date,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "Tm".to_owned(),
                DataType::Time,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "ival".to_owned(),
                DataType::Interval,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "tstamp".to_owned(),
                DataType::Timestamp,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "uid".to_owned(),
                DataType::Uuid,
                false,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "hash".to_owned(),
                DataType::Map,
                true,
                "".to_owned(),
                "".to_owned()
            ),
            (
                "glist".to_owned(),
                DataType::List,
                false,
                "".to_owned(),
                "".to_owned()
            )
        ]))
    );

    test!(
        r#"EXPLAIN mytable1"#,
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into())
    );
});
