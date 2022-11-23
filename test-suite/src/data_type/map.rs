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
        "
CREATE TABLE MapType (
    id INTEGER NULL DEFAULT UNWRAP(NULL, 'a'),
    nested MAP
)"
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
        "SELECT
            id,
            UNWRAP(nested, 'a.foo') || '.yeah' AS foo,
            UNWRAP(nested, 'a.b.c.d') as good,
            UNWRAP(nested, 'a.b.c.d') * 2 as good2,
            UNWRAP(nested, 'a.b') as b
        FROM MapType",
        Ok(select_with_null!(
            id     | foo          | good    | good2   | b;
            I64(1)   Null           Null      Null      Null;
            I64(2)   s("ok.yeah")   Null      Null      s("steak");
            I64(3)   Null           I64(10)   I64(20)   m(r#"{"c": { "d": 10 } }"#)
        ))
    );

    test!(
        "SELECT
            id,
            UNWRAP(NULL, 'a.b') as foo,
            UNWRAP(nested, NULL) as bar
        FROM MapType LIMIT 1",
        Ok(select_with_null!(id | foo | bar; I64(1) Null Null))
    );

    run!(
        "
CREATE TABLE MapType2 (
    id INTEGER,
    nested MAP
)"
    );

    run!(
        r#"
INSERT INTO MapType2 VALUES
    (1, '{"a": {"red": "apple", "blue": 1}, "b": 10}'),
    (2, '{"a": {"red": "cherry", "blue": 2}, "b": 20}'),
    (3, '{"a": {"red": "berry", "blue": 3}, "b": 30, "c": true}');
"#
    );

    test!(
        "SELECT id, nested['b'] as b FROM MapType2",
        Ok(select_with_null!(
            id     | b;
            I64(1)   I64(10);
            I64(2)   I64(20);
            I64(3)   I64(30)
        ))
    );

    test! {
        name: "select index expr without alias",
        sql: "SELECT id, nested['b'] FROM MapType2",
        expected: Ok(select_with_null!(
            id     | "nested['b']";
            I64(1)   I64(10);
            I64(2)   I64(20);
            I64(3)   I64(30)
        ))
    }

    test! {
        name: "index expr with non-existent key from MapType Value returns Null",
        sql: "SELECT
            id,
            nested['a']['red'] AS fruit,
            nested['a']['blue'] + nested['b'] as sum,
            nested['c'] AS c
        FROM MapType2",
        expected: Ok(select_with_null!(
                id     | fruit        | sum      | c;
                I64(1)   s("apple")     I64(11)    Null;
                I64(2)   s("cherry")    I64(22)    Null;
                I64(3)   s("berry")     I64(33)    Bool(true)
        ))
    }

    test!(
        "SELECT UNWRAP('abc', 'a.b.c') FROM MapType",
        Err(EvaluateError::FunctionRequiresMapValue("UNWRAP".to_owned()).into())
    );
    test!(
        "SELECT UNWRAP(id, 'a.b.c') FROM MapType",
        Err(ValueError::SelectorRequiresMapOrListTypes.into())
    );
    test!(
        "SELECT id, nested['a']['blue']['first'] FROM MapType2",
        Err(ValueError::SelectorRequiresMapOrListTypes.into())
    );
    test!(
        "SELECT id FROM MapType GROUP BY nested",
        Err(KeyError::MapTypeKeyNotSupported.into())
    );
    test!(
        "INSERT INTO MapType VALUES (1, '{{ ok [1, 2, 3] }');",
        Err(ValueError::InvalidJsonString("{{ ok [1, 2, 3] }".to_owned()).into())
    );
    test!(
        "INSERT INTO MapType VALUES (1, '[1, 2, 3]');",
        Err(ValueError::JsonObjectTypeRequired.into())
    );
});
