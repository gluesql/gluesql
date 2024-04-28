use {
    crate::*,
    gluesql_core::{
        error::TranslateError,
        prelude::{Payload::ShowVariable, PayloadVariable, Value::*},
    },
};

test_case!(dictionary, {
    let g = get_tester!();

    let tables = |v: Vec<&str>| {
        Ok(ShowVariable(PayloadVariable::Tables(
            v.into_iter().map(ToOwned::to_owned).collect(),
        )))
    };

    assert!(matches!(
        g.run("SHOW VERSION;").await,
        ShowVariable(PayloadVariable::Version(_))
    ));

    g.test("SHOW TABLES", tables(Vec::new())).await;

    g.run("CREATE TABLE Foo (id INTEGER, name TEXT NULL, type TEXT NULL) COMMENT='this is table comment';")
        .await;
    g.test("SHOW TABLES", tables(vec!["Foo"])).await;

    g.run("CREATE TABLE Zoo (id INTEGER PRIMARY KEY COMMENT 'hello');")
        .await;
    g.run("CREATE TABLE Bar (id INTEGER UNIQUE, name TEXT NOT NULL DEFAULT 'NONE');")
        .await;

    g.test("SHOW TABLES", tables(vec!["Bar", "Foo", "Zoo"]))
        .await;

    g.test(
        "SHOW WHATEVER",
        Err(TranslateError::UnsupportedShowVariableKeyword("WHATEVER".to_owned()).into()),
    )
    .await;

    g.test(
        "SHOW ME THE CHICKEN",
        Err(
            TranslateError::UnsupportedShowVariableStatement("SHOW ME THE CHICKEN".to_owned())
                .into(),
        ),
    )
    .await;

    g.test(
        "SELECT * FROM GLUE_TABLES",
        Ok(select_with_null!(
            TABLE_NAME            | COMMENT;
            Str("Bar".to_owned())   Null;
            Str("Foo".to_owned())   Str("this is table comment".to_owned());
            Str("Zoo".to_owned())   Null
        )),
    )
    .await;

    let s = |v: &str| Str(v.to_owned());

    g.test(
        "SELECT * FROM GLUE_TABLE_COLUMNS",
        Ok(select_with_null!(
            TABLE_NAME | COLUMN_NAME | COLUMN_ID | NULLABLE    | KEY              | DEFAULT     | COMMENT;
            s("Bar")     s("id")       I64(1)      Bool(true)    s("UNIQUE")        Null          Null;
            s("Bar")     s("name")     I64(2)      Bool(false)   Null               s("'NONE'")   Null;
            s("Foo")     s("id")       I64(1)      Bool(true)    Null               Null          Null;
            s("Foo")     s("name")     I64(2)      Bool(true)    Null               Null          Null;
            s("Foo")     s("type")     I64(3)      Bool(true)    Null               Null          Null;
            s("Zoo")     s("id")       I64(1)      Bool(false)   s("PRIMARY KEY")   Null          s("hello") 
        ))
    ).await;
});
