use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*, translate::TranslateError},
};

test_case!(ascii, async move {
    test!(
        r#"VALUES(ASCII("A"))"#,
        Ok(select!(
            column1
            I64;
            65
        ))
    );
    test!(
        r#"VALUES(ASCII("AB"))"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );
    run!(
        "
        CREATE TABLE Ascii (
            id INTEGER,
            text TEXT
        );
    "
    );
    run!(r#"INSERT INTO Ascii VALUES (1, "F");"#);
    test!(
        r#"select ascii(text) as ascii from Ascii;"#,
        Ok(select!(
            ascii
            I64;
            70
        ))
    );

    test!(
        r#"select ascii("a") as ascii from Ascii;"#,
        Ok(select!(
           ascii
           I64;
           97
        ))
    );

    test!(
        r#"select ascii("A") as ascii from Ascii;"#,
        Ok(select!(
           ascii
           I64;
           65
        ))
    );

    test!(
        r#"select ascii("ab") as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );

    test!(
        r#"select ascii("AB") as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );

    test!(
        r#"select ascii("") as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );

    test!(
        r#"select ascii("ukjhg") as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );

    test!(
        r#"select ascii(NULL) as ascii from Ascii;"#,
        Ok(select_with_null!(ascii; Null))
    );

    test!(
        r#"select ascii("ㄱ") as ascii from Ascii;"#,
        Err(EvaluateError::NonAsciiCharacterNotAllowed.into())
    );

    test!(
        r#"select ascii() as ascii from Ascii;"#,
        Err(TranslateError::FunctionArgsLengthNotMatching {
            name: "ASCII".to_owned(),
            expected: 1,
            found: 0
        }
        .into())
    );

    run!(r#"INSERT INTO Ascii VALUES (1, "Foo");"#);

    test!(
        r#"select ascii(text) as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );
});
