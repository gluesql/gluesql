use crate::*;

test_case!(case, async move {
    use Value::{Null, Str, I64};
    let test_cases = vec![
        (
            "CREATE TABLE Item (id INTEGER, name TEXT);",
            Ok(Payload::Create),
        ),
        (
            r#"
            INSERT INTO 
            Item (id, name)
            VALUES
                (1, "Harry"), (2, "Ron"), (3, "Hermione");
            "#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"
            SELECT CASE id
                WHEN 1 THEN name
                WHEN 2 THEN name 
                WHEN 4 THEN name 
                ELSE "Malfoy" END
            AS case FROM Item;
            "#,
            Ok(select!(
                case
                Str;
                "Harry".to_owned();
                "Ron".to_owned();
                "Malfoy".to_owned()
            )),
        ),
        (
            r#"
            SELECT CASE id
                WHEN 1 THEN name
                WHEN 2 THEN name 
                WHEN 4 THEN name 
                END
            AS case FROM Item;
            "#,
            Ok(select_with_null!(
                "case";
                Str("Harry".to_owned());
                Str("Ron".to_owned());
                Null
            )),
        ),
        (
            r#"
            SELECT CASE
                WHEN name = "Harry" THEN id
                WHEN name = "Ron" THEN id
                WHEN name = "Hermione" THEN id
                ELSE 404 END
            AS case FROM Item;
            "#,
            Ok(select!(
                case
                I64;
                1;
                2;
                3
            )),
        ),
        (
            r#"
            SELECT CASE
                WHEN name = "Harry" THEN id
                WHEN name = "Ron" THEN id 
                WHEN name = "Hermion" THEN id 
                END
            AS case FROM Item;
            "#,
            Ok(select_with_null!(
                "case";
                I64(1);
                I64(2);
                Null
            )),
        ),
        (
            r#"
            SELECT CASE
                WHEN (name = "Harry") OR (name = "Ron") THEN (id + 1)
                WHEN name = ("Hermi" || "one") THEN (id + 2)
                ELSE 404 END
            AS case FROM Item;
            "#,
            Ok(select!(
                case
                I64;
                2;
                3;
                5
            )),
        ),
        (
            r#"
            SELECT CASE 1 COLLATE Item
                WHEN name = "Harry" THEN id
                WHEN name = "Ron" THEN id 
                WHEN "Hermione" THEN id 
                END
            AS case FROM Item;
            "#,
            Err(TranslateError::UnsupportedExpr("1 COLLATE Item".to_owned()).into()),
        ),
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
