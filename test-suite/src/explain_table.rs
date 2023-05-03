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
        CREATE TABLE person(
            id INT PRIMARY KEY,
            name TEXT,
            age INT NOT NULL,
            alive BOOLEAN DEFAULT true
        )
    "
    );

    test!(
        r#"EXPLAIN person"#,
        Ok(Payload::ExplainTable(vec![
            (
                "id".to_owned(),
                DataType::Int,
                false,
                "PRIMARY KEY".to_owned(),
                "".to_owned(),
                "".to_owned()
            ),
            (
                "name".to_owned(),
                DataType::Text,
                true,
                "".to_owned(),
                "".to_owned(),
                "".to_owned()
            ),
            (
                "age".to_owned(),
                DataType::Int,
                false,
                "".to_owned(),
                "".to_owned(),
                "".to_owned()
            ),
            (
                "alive".to_owned(),
                DataType::Boolean,
                true,
                "".to_owned(),
                "TRUE".to_owned(),
                "".to_owned()
            )
        ]))
    );

    test!(
        r#"EXPLAIN mytable1"#,
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into())
    );
});
