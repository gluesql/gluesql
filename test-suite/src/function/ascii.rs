use {
    crate::*,
    gluesql_core::{executor::EvaluateError, prelude::Value::*},
};

test_case!(ascii, async move {
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

    run!(r#"INSERT INTO Ascii VALUES (1, "Foo");"#);

    test!(
        r#"select ascii(text) as ascii from Ascii;"#,
        Err(EvaluateError::AsciiFunctionRequiresSingleCharacterValue.into())
    );
});
