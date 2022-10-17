use {
    crate::*,
    gluesql_core::{
        data::{KeyError, ValueError},
        prelude::Value::{self, *},
    },
};

test_case!(list, async move {
    run!(
        r#"
CREATE TABLE ListType (
    id INTEGER,
    items LIST
)"#
    );

    run!(
        r#"
INSERT INTO ListType VALUES
    (1, "[1, 2, 3]"),
    (2, '["hello", "world", 30, true, [9,8]]'),
    (3, '[{ "foo": 100, "bar": [true, 0,[10.5, false] ] }, 10, 20]');
"#
    );

    let l = |s: &str| Value::parse_json_list(s).unwrap();
    let s = |v: &str| Str(v.to_owned());

    test!(
        "SELECT id, items FROM ListType",
        Ok(select_with_null!(
            id     | items;
            I64(1)   l("[1,2,3]");
            I64(2)   l(r#"["hello","world",30,true,[9,8]]"#);
            I64(3)   l(r#"[{"foo":100, "bar": [true, 0, [10.5, false]]},10,20]"#)
        ))
    );

    test!(
        r#"SELECT
            id,
            UNWRAP(items, "1") AS foo,
            UNWRAP(items, "0.foo") + 100 AS bar,
            UNWRAP(items, "4") AS a,
            UNWRAP(items, "0.bar.2.0") + UNWRAP(items, "2") AS b
        FROM ListType"#,
        Ok(select_with_null!(
            id     | foo        | bar      | a             | b;
            I64(1)   I64(2)       Null       Null            Null;
            I64(2)   s("world")   Null       l(r#"[9,8]"#)   Null;
            I64(3)   I64(10)      I64(200)   Null            F64(30.5)
        ))
    );

    test!(
        "SELECT id, items[1] AS second FROM ListType",
        Ok(select_with_null!(
            id     | second;
            I64(1)   I64(2);
            I64(2)   s("world");
            I64(3)   I64(10)
        ))
    );

    test! {
        name: "select index expr without alias",
        sql: "SELECT id, items[1] FROM ListType",
        expected: Ok(select_with_null!(
            id     | "items[1]";
            I64(1)   I64(2);
            I64(2)   s("world");
            I64(3)   I64(10)
        ))
    }

    run!(
        r#"
CREATE TABLE ListType2 (
    id INTEGER,
    items LIST
)"#
    );

    run!(
        r#"
INSERT INTO ListType2 VALUES
    (1, '[1, 2, 3, { "hi": "bye" }]'),
    (2, '["one", "two", "three", [100, 200]]'),
    (3, '["first", "second", "third", { "foo": true, "bar": false }]');
"#
    );

    test!(
        r#"SELECT
            id,
            items["0"] AS foo,
            items["1"] AS bar,
            items["3"]["0"] AS hundred
        FROM ListType2"#,
        Ok(select_with_null!(
            id     | foo        | bar        | hundred;
            I64(1)   I64(1)       I64(2)       Null;
            I64(2)   s("one")     s("two")     I64(100);
            I64(3)   s("first")   s("second")  Null
        ))
    );

    test!(
        r#"SELECT id, items["not"]["list"] AS foo FROM ListType2"#,
        Err(ValueError::SelectorRequiresMapOrListTypes.into())
    );

    test!(
        r#"SELECT id FROM ListType GROUP BY items"#,
        Err(KeyError::ListTypeKeyNotSupported.into())
    );
    test!(
        r#"INSERT INTO ListType VALUES (1, '{ "a": 10 }');"#,
        Err(ValueError::JsonArrayTypeRequired.into())
    );
    test!(
        r#"INSERT INTO ListType VALUES (1, '{{ ok [1, 2, 3] }');"#,
        Err(ValueError::InvalidJsonString.into())
    );
});
