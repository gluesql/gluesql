use {
    crate::*,
    gluesql_core::{
        error::{EvaluateError, TranslateError},
        prelude::Value::*,
    },
};

test_case!(ascii, {
    let g = get_tester!();

    g.test(
        "VALUES(ASCII('A'))",
        Ok(select!(
            column1
            U8;
            65
        )),
    )
    .await;
    g.test(
        "VALUES(ASCII('AB'))",
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;
    g.run(
        "
        CREATE TABLE Ascii (
            id INTEGER,
            text TEXT
        );
    ",
    )
    .await;
    g.run("INSERT INTO Ascii VALUES (1, 'F');").await;
    g.test(
        r#"select ascii(text) as ascii from Ascii;"#,
        Ok(select!(
            ascii
            U8;
            70
        )),
    )
    .await;

    g.test(
        "select ascii('a') as ascii from Ascii;",
        Ok(select!(
           ascii
           U8;
           97
        )),
    )
    .await;

    g.test(
        "select ascii('A') as ascii from Ascii;",
        Ok(select!(
           ascii
           U8;
           65
        )),
    )
    .await;

    g.test(
        "select ascii('ab') as ascii from Ascii;",
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;

    g.test(
        "select ascii('AB') as ascii from Ascii;",
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;

    g.test(
        "select ascii('') as ascii from Ascii;",
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;

    g.test(
        "select ascii('ukjhg') as ascii from Ascii;",
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;

    g.test(
        r#"select ascii(NULL) as ascii from Ascii;"#,
        Ok(select_with_null!(ascii; Null)),
    )
    .await;

    g.test(
        "select ascii('ㄱ') as ascii from Ascii;",
        Err(EvaluateError::NonAsciiCharacterNotAllowed.into()),
    )
    .await;

    g.test(
        r#"select ascii() as ascii from Ascii;"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "ASCII".to_owned(),
            expected: 1,
            found: 0,
        }
        .into()),
    )
    .await;

    g.run("INSERT INTO Ascii VALUES (1, 'Foo');").await;

    g.test(
        r#"select ascii(text) as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into()),
    )
    .await;
});
