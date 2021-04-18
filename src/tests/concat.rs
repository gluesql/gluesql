use crate::*;

test_case!(concat, async move {
    use Value::{Null, Str};

    run!(
        "
        CREATE TABLE Concat (
            id INTEGER,
            rate FLOAT,
            flag BOOLEAN,
            text TEXT,
            null_value TEXT NULL,
        );
    "
    );
    run!(r#"INSERT INTO Concat VALUES (1, 2.3, TRUE, "Foo", NULL);"#);

    test!(
        Ok(select!(
            value_value         | value_literal       | literal_value       | literal_literal
            Str                 | Str                 | Str                 | Str;
            "FooFoo".to_owned()   "FooBar".to_owned()   "BarFoo".to_owned()   "FooBar".to_owned()
        )),
        r#"
        SELECT
            text || text AS value_value,
            text || "Bar" AS value_literal,
            "Bar" || text AS literal_value,
            "Foo" || "Bar" AS literal_literal
        FROM Concat;
        "#
    );

    test!(
        Ok(select_with_null!(
            id_n | rate_n | flag_n | text_n | n_id | n_text;
            Null   Null     Null     Null     Null   Null
        )),
        "SELECT
            id || null_value AS id_n,
            rate || null_value AS rate_n,
            flag || null_value AS flag_n,
            text || null_value AS text_n,
            null_value || id AS n_id,
            null_value || text AS n_text
        FROM
            Concat;"
    );

    test!(
        Ok(select!(
            "id || rate"      | "rate || flag"       | "flag || text"       | "id || text"
            Str               | Str                  | Str                  | Str;
            "12.3".to_owned()   "2.3TRUE".to_owned()   "TRUEFoo".to_owned()   "1Foo".to_owned()
        )),
        "SELECT
            id || rate,
            rate || flag,
            flag || text,
            id || text
        FROM
            Concat;"
    );

    test!(
        Ok(select!(
            int_float         | float_bool           | bool_text             | int_text
            Str               | Str                  | Str                   | Str;
            "12.3".to_owned()   "2.3TRUE".to_owned()   "FALSEFoo".to_owned()   "1Bar".to_owned()
        )),
        r#"SELECT
            1 || 2.3 AS int_float,
            2.3 || TRUE AS float_bool,
            FALSE || "Foo" AS bool_text,
            1 || "Bar" AS int_text
        FROM
            Concat;"#
    );

    test!(
        Ok(select_with_null!(
            Case1                      | Case2                         | Case3;
            Str("112.3Bar".to_owned())   Str("1TRUE3.5Foo".to_owned())   Null
        )),
        r#"SELECT
            1 || id || rate || "Bar" AS Case1,
            id || flag || 3.5 || text AS Case2,
            flag || "wow" || null_value AS Case3
        FROM
            Concat;"#
    );
});
