#![cfg(feature = "metadata")]

use {
    crate::*,
    gluesql_core::{
        prelude::{Payload::ShowVariable, PayloadVariable},
        translate::TranslateError,
    },
};

test_case!(metadata, async move {
    let tables = |v: Vec<&str>| {
        Ok(ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    assert!(matches!(
        run!("SHOW VERSION;"),
        ShowVariable(PayloadVariable::Version(_))
    ));

    test!("SHOW TABLES", tables(Vec::new()));

    run!("CREATE TABLE Foo (id INTEGER);");
    test!("SHOW TABLES", tables(vec!["Foo"]));

    run!("CREATE TABLE Zoo (id INTEGER);");
    run!("CREATE TABLE Bar (id INTEGER);");
    test!("SHOW TABLES", tables(vec!["Bar", "Foo", "Zoo"]));

    test!(
        "SHOW WHATEVER",
        Err(TranslateError::UnsupportedShowVariableKeyword("WHATEVER".to_owned()).into())
    );

    test!(
        "SHOW ME THE CHICKEN",
        Err(
            TranslateError::UnsupportedShowVariableStatement("SHOW ME THE CHICKEN".to_owned())
                .into()
        )
    );
});
