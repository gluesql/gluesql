use {
    crate::*,
    gluesql_core::{
        ast::DataType,
        executor::{ExecuteError, ExplainTableRow, Payload},
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
            ExplainTableRow {
                name: "id".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                key: "PRIMARY KEY".to_owned(),
                default: "".to_owned(),
            },
            ExplainTableRow {
                name: "name".to_owned(),
                data_type: DataType::Text,
                nullable: true,
                key: "".to_owned(),
                default: "".to_owned(),
            },
            ExplainTableRow {
                name: "age".to_owned(),
                data_type: DataType::Int,
                nullable: false,
                key: "".to_owned(),
                default: "".to_owned(),
            },
            ExplainTableRow {
                name: "alive".to_owned(),
                data_type: DataType::Boolean,
                nullable: true,
                key: "".to_owned(),
                default: "TRUE".to_owned(),
            }
        ]))
    );

    test!(
        r#"EXPLAIN mytable1"#,
        Err(ExecuteError::TableNotFound("mytable1".to_owned()).into())
    );
});
