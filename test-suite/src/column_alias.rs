use {
    crate::*,
    gluesql_core::{
        executor::FetchError,
        prelude::{Payload, Value::*},
    },
};

test_case!(column_alias, async move {
    let test_cases = [
        (
            "CREATE TABLE InnerTable (
                id INTEGER,
                name TEXT 
            )",
            Ok(Payload::Create),
        ),
        (
            "CREATE TABLE User (
                id INTEGER,
                name TEXT
            )",
            Ok(Payload::Create),
        ),
        ("CREATE TABLE EmptyTable", Ok(Payload::Create)),
        (
            "INSERT INTO InnerTable VALUES (1, 'GLUE'), (2, 'SQL'), (3, 'SQL')",
            Ok(Payload::Insert(3)),
        ),
        (
            "INSERT INTO User VALUES (1, 'Taehoon'), (2, 'Mike'), (3, 'Jorno')",
            Ok(Payload::Insert(3)),
        ),
        (
            "SELECT * FROM InnerTable",
            Ok(select!(
                    id  | name
                    I64 | Str;
                    1     "GLUE".to_owned();
                    2     "SQL".to_owned();
                    3     "SQL".to_owned()
            )),
        ),
        (
            // column alias with wildcard
            "SELECT * FROM User AS Table(a, b)",
            Ok(select!(
                a   | b
                I64 | Str;
                1     "Taehoon".to_owned();
                2     "Mike".to_owned();
                3     "Jorno".to_owned()
            )),
        ),
        (
            // partial column alias
            "SELECT * FROM User AS Table(a)",
            Ok(select!(
                a   | name
                I64 | Str;
                1     "Taehoon".to_owned();
                2     "Mike".to_owned();
                3     "Jorno".to_owned()
            )),
        ),
        (
            // column alias (non-wildcard)
            "SELECT a FROM User AS Table(a, b)",
            Ok(select!( a; I64; 1; 2; 3)),
        ),
        (
            // too many column alias
            "Select * from User as Table(a, b, c)",
            Err(FetchError::TooManyColumnAliases("User".to_owned(), 2, 3).into()),
        ),
        // InlineView
        (
            // column alias with wildcard
            "SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b)",
            Ok(select!(
                    a   | b
                    I64 | Str;
                    1     "GLUE".to_owned();
                    2     "SQL".to_owned();
                    3     "SQL".to_owned()
            )),
        ),
        (
            // partial column alias
            "SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a)",
            Ok(select!(
                    a   | name
                    I64 | Str;
                    1     "GLUE".to_owned();
                    2     "SQL".to_owned();
                    3     "SQL".to_owned()
            )),
        ),
        (
            // too many column alias
            "SELECT * FROM (SELECT * FROM InnerTable) AS InlineView(a, b, c)",
            Err(FetchError::TooManyColumnAliases("InlineView".into(), 2, 3).into()),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id)",
            Ok(select!(
                id      | column2;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name)",
            Ok(select!(
                id      | name;
                I64     | Str;
                1         "a".to_owned();
                2         "b".to_owned()
            )),
        ),
        (
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived(id, name, dummy)",
            Err(FetchError::TooManyColumnAliases("Derived".into(), 2, 3).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
