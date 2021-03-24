use crate::*;

test_case!(types, async move {
    run!("CREATE TABLE TableB (id BOOL);");
    test!(
        Err(ValueError::SqlTypeNotSupported.into()),
        "INSERT INTO TableB (id) VALUES (0);"
    );

    run!("CREATE TABLE TableC (id INTEGER UNIQUE);");
    run!("INSERT INTO TableC (id) VALUES (1);");
    test!(
        Err(ValidateError::IncompatibleTypeOnTypedField {
            attempted_value: "Str(\"A\")".to_string(),
            column_name: "id".to_string(),
            column_type: "INT".to_string()
        }
        .into()),
        "INSERT INTO TableC (id) VALUES (\"A\")"
    );
});
