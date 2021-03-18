use {crate::*, std::borrow::Cow};

test_case!(types, async move {
    run!("CREATE TABLE TableB (id BOOL);");
    run!("CREATE TABLE TableC (uid INTEGER UNIQUE);");
    run!("INSERT INTO TableC (uid) VALUES (1);");
    test!(
        Err(ValidateError::IncompatibleTypeOnTypedField {
            attempted_value: format!("{:?}", Value::I64(1)),
            column_name: "id".to_owned(),
            column_type: "BOOL".to_owned(),
        }
        .into()),
        "INSERT INTO TableB SELECT uid FROM TableC;"
    );
    test!(
        Err(ValueError::IncompatibleLiteralForDataType {
            data_type: "INT".to_owned(),
            literal: format!("{:?}", data::Literal::Text(Cow::Owned("A".to_owned()))),
        }
        .into()),
        "INSERT INTO TableC (uid) VALUES (\"A\")"
    );
});
