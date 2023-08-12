use {
    crate::*,
    gluesql_core::{
        error::EvaluateError,
        prelude::{Payload, Value::*},
    },
};
test_case!(append, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Append (
            id INTEGER,
            items LIST,
            element INTEGER,
            element2 TEXT
        );
    ",
    )
    .await;
    g.run(
        r#"
            INSERT INTO Append VALUES
            (1, '[1, 2, 3]', 4, 'Foo');
        "#,
    )
    .await;
    g.test(
        r#"select append(items, element) as myappend from Append;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), I64(4)]
        )),
    )
    .await;
    g.test(
        r#"select append(items, element2) as myappend from Append;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), Str("Foo".into())]
        )),
    )
    .await;

    g.test(
        r#"select append(element, element2) as myappend from Append"#,
        Err(EvaluateError::ListTypeRequired.into()),
    )
    .await;

    g.test(
        r#"CREATE TABLE Foo (
                elements LIST
            );"#,
        Ok(Payload::Create),
    )
    .await;

    g.run(
        r#"
            INSERT INTO Foo VALUES (APPEND(CAST('[1, 2, 3]' AS LIST), 4));
        "#,
    )
    .await;
    g.test(
        r#"select elements as myappend from Foo;"#,
        Ok(select!(
           myappend
           List;
           vec![I64(1), I64(2), I64(3), I64(4)]
        )),
    )
    .await;
});
