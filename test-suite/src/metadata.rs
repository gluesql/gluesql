#![cfg(feature = "metadata")]

use {
    crate::*,
    gluesql_core::{
        prelude::{Payload::ShowVariable, PayloadVariable, Value::*},
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

    test!(
        "SELECT * FROM GLUE_TABLES ORDER BY TABLE_NAME",
        Ok(select!(
            TABLE_NAME;
            Str;
            "Bar".to_owned();
            "Foo".to_owned();
            "Zoo".to_owned()
        ))
    );

    run!("ALTER TABLE Bar ADD COLUMN name TEXT NULL");
    run!("ALTER TABLE Foo ADD COLUMN name TEXT NULL");
    run!("ALTER TABLE Foo ADD COLUMN type TEXT NULL");
    test!(
        "SELECT * FROM GLUE_TABLE_COLUMNS ORDER BY TABLE_NAME",
        Ok(select!(
            TABLE_NAME       | COLUMN_NAME      | COLUMN_ID;
            Str              | Str              | I64;
            "Bar".to_owned()   "id".to_owned()    1;
            "Bar".to_owned()   "name".to_owned()  2;
            "Foo".to_owned()   "id".to_owned()    1;
            "Foo".to_owned()   "name".to_owned()  2;
            "Foo".to_owned()   "type".to_owned()  3;
            "Zoo".to_owned()   "id".to_owned()    1
        ))
    );
});
