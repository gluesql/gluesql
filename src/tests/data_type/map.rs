use crate::{EvaluateError, Value::*, *};

test_case!(map, async move {
    run!(
        r#"
CREATE TABLE MapType (
    id INTEGER,
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

    let m = |s: &str| Map(data::Map::parse_json(s).unwrap());
    let s = |v: &str| Str(v.to_owned());

    test!(
        Ok(select_with_null!(
            id     | nested;
            I64(1)   m(r#"{"a": true, "b": 2}"#)
        )),
        "SELECT id, nested FROM MapType LIMIT 1"
    );

    test!(
        Ok(select_with_null!(
            id     | foo          | good    | good2   | b;
            I64(1)   Null           Null      Null      Null;
            I64(2)   s("ok.yeah")   Null      Null      s("steak");
            I64(3)   Null           I64(10)   I64(20)   m(r#"{"c": { "d": 10 } }"#)
        )),
        r#"SELECT
            id,
            UNWRAP(nested, "a.foo") || ".yeah" AS foo,
            UNWRAP(nested, 'a.b.c.d') as good,
            UNWRAP(nested, 'a.b.c.d') * 2 as good2,
            UNWRAP(nested, "a.b") as b
        FROM MapType"#
    );

    test!(
        Ok(select_with_null!(id | foo | bar; I64(1) Null Null)),
        r#"SELECT
            id,
            UNWRAP(NULL, "a.b") as foo,
            UNWRAP(nested, NULL) as bar
        FROM MapType LIMIT 1"#
    );

    test!(
        Err(EvaluateError::FunctionRequiresMapValue("UNWRAP".to_owned()).into()),
        r#"SELECT UNWRAP(id, "a.b.c") FROM MapType"#
    );
    test!(
        Err(ValueError::GroupByNotSupported("MAP".to_owned()).into()),
        r#"SELECT id FROM MapType GROUP BY nested"#
    );
    test!(
        Err(MapError::ArrayTypeNotSupported.into()),
        r#"INSERT INTO MapType VALUES (1, '{ "a": [1, 2, 3] }');"#
    );
    test!(
        Err(MapError::InvalidJsonString.into()),
        r#"INSERT INTO MapType VALUES (1, '{{ ok [1, 2, 3] }');"#
    );
    test!(
        Err(MapError::ObjectTypeJsonRequired.into()),
        r#"INSERT INTO MapType VALUES (1, '[1, 2, 3]');"#
    );
});
