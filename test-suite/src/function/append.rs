use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Payload, prelude::Value::*},
};
test_case!(append, async move {
    run!(
        "
        CREATE TABLE Append (
            id INTEGER,
            items LIST,
            element INTEGER,
            element2 TEXT
        );
    "
    );
    run!(
        r#"
            INSERT INTO Append VALUES
            (1, '[1, 2, 3]', 4, 'Foo');
        "#
    );
    test!(
        r#"select append(items, element) as myappend from Append;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), I64(4)]
        ))
    );
    test!(
        r#"select append(items, element2) as myappend from Append;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), Str("Foo".into())]
        ))
    );

    test!(
        r#"select append(element, element2) as myappend from Append"#,
        Err(EvaluateError::ListTypeRequired.into())
    );

    test!(
        r#"CREATE TABLE Foo (
                elements LIST
            );"#,
        Ok(Payload::Create)
    );

    run!(
        r#"
            INSERT INTO Foo VALUES (APPEND(CAST('[1, 2, 3]' AS LIST), 4));
        "#
    );
    test!(
        r#"select elements as myappend from Foo;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), I64(4)]
        ))
    );
});
