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

    test!(tables(Vec::new()), "SHOW TABLES");

    run!("CREATE TABLE Foo (id INTEGER);");
    test!(tables(vec!["Foo"]), "SHOW TABLES");

    run!("CREATE TABLE Zoo (id INTEGER);");
    run!("CREATE TABLE Bar (id INTEGER);");
    test!(tables(vec!["Bar", "Foo", "Zoo"]), "SHOW TABLES");

    test!(
        Err(TranslateError::UnsupportedShowVariableKeyword("WHATEVER".to_owned()).into()),
        "SHOW WHATEVER"
    );

    test!(
        Err(
            TranslateError::UnsupportedShowVariableStatement("SHOW ME THE CHICKEN".to_owned())
                .into()
        ),
        "SHOW ME THE CHICKEN"
    );
});
