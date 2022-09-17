use {
    crate::*,
    gluesql_core::{
        data::{KeyError, ValueError},
        executor::EvaluateError,
        prelude::Value::{self, *},
    },
};

test_case!(map, async move {
    run!(
        r#"
CREATE TABLE MapType (
    id INTEGER NULL DEFAULT UNWRAP(NULL, "a"),
    nested MAP
)"#
    );

    run!(
        r#"
INSERT INTO MapType VALUES
    (1, '{"a": true, "b": 2}'),
    (2, '{"a": {"foo": "ok", "b": "steak"}, "b": 30}'),
    (3, '{"a": {"b": {"c": {"d": 10}}}}');
"#
    );

    let m = |s: &str| Value::parse_json_map(s).unwrap();
    let s = |v: &str| Str(v.to_owned());

    test!(
        "SELECT id, nested FROM MapType LIMIT 1",
        Ok(select_with_null!(
            id     | nested;
            I64(1)   m(r#"{"a": true, "b": 2}"#)
        ))
    );

    test!(
        r#"SELECT
            id,
            UNWRAP(nested, "a.foo") || ".yeah" AS foo,
            UNWRAP(nested, 'a.b.c.d') as good,
            UNWRAP(nested, 'a.b.c.d') * 2 as good2,
            UNWRAP(nested, "a.b") as b
        FROM MapType"#,
        Ok(select_with_null!(
            id     | foo          | good    | good2   | b;
            I64(1)   Null           Null      Null      Null;
            I64(2)   s("ok.yeah")   Null      Null      s("steak");
            I64(3)   Null           I64(10)   I64(20)   m(r#"{"c": { "d": 10 } }"#)
        ))
    );

    test!(
        r#"SELECT
            id,
            UNWRAP(NULL, "a.b") as foo,
            UNWRAP(nested, NULL) as bar
        FROM MapType LIMIT 1"#,
        Ok(select_with_null!(id | foo | bar; I64(1) Null Null))
    );

    // TODO add arrayindex test case
    test!(
        r#"SELECT
            id,
            nested[a][foo] AS foo,
            nested[a][b][c][d] as good,
            nested[a][b] AS b
        FROM MapType"#,
        Ok(select_with_null!(
            id     | foo          | good    | b;
            I64(1)   Null           Null      Null;
            I64(2)   s("ok")        Null      s("steak");
            I64(3)   Null           I64(10)   m(r#"{"c": { "d": 10 } }"#)
        ))
    );

    test!(
        r#"SELECT UNWRAP("abc", "a.b.c") FROM MapType"#,
        Err(EvaluateError::FunctionRequiresMapValue("UNWRAP".to_owned()).into())
    );
    test!(
        r#"SELECT UNWRAP(id, "a.b.c") FROM MapType"#,
        Err(ValueError::SelectorRequiresMapOrListTypes.into())
    );
    test!(
        r#"SELECT id FROM MapType GROUP BY nested"#,
        Err(KeyError::MapTypeKeyNotSupported.into())
    );
    test!(
        r#"INSERT INTO MapType VALUES (1, '{{ ok [1, 2, 3] }');"#,
        Err(ValueError::InvalidJsonString.into())
    );
    test!(
        r#"INSERT INTO MapType VALUES (1, '[1, 2, 3]');"#,
        Err(ValueError::JsonObjectTypeRequired.into())
    );
});
