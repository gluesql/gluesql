use crate::*;
use test::*;

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
        Ok(select_with_null!(
            id     | items;
            I64(1)   l("[1,2,3]");
            I64(2)   l(r#"["hello","world",30,true,[9,8]]"#);
            I64(3)   l(r#"[{"foo":100, "bar": [true, 0, [10.5, false]]},10,20]"#)
        )),
        "SELECT id, items FROM ListType"
    );

    test!(
        Ok(select_with_null!(
            id     | foo        | bar      | a             | b;
            I64(1)   I64(2)       Null       Null            Null;
            I64(2)   s("world")   Null       l(r#"[9,8]"#)   Null;
            I64(3)   I64(10)      I64(200)   Null            F64(30.5)
        )),
        r#"SELECT
            id,
            UNWRAP(items, "1") AS foo,
            UNWRAP(items, "0.foo") + 100 AS bar,
            UNWRAP(items, "4") AS a,
            UNWRAP(items, "0.bar.2.0") + UNWRAP(items, "2") AS b
        FROM ListType"#
    );

    test!(
        Err(ValueError::GroupByNotSupported("LIST".to_owned()).into()),
        r#"SELECT id FROM ListType GROUP BY items"#
    );
    test!(
        Err(ValueError::JsonArrayTypeRequired.into()),
        r#"INSERT INTO ListType VALUES (1, '{ "a": 10 }');"#
    );
    test!(
        Err(ValueError::InvalidJsonString.into()),
        r#"INSERT INTO ListType VALUES (1, '{{ ok [1, 2, 3] }');"#
    );
});
