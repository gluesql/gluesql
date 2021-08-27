use crate::*;

test_case!(case, async move {
    use Value::{Str, I64};
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
                WHEN 3 THEN name 
                ELSE "Malfoy" END
            AS case FROM Item;
            "#,
            Ok(select!(
                case
                Str;
                "Harry".to_owned();
                "Ron".to_owned();
                "Hermione".to_owned()
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
    ];
    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
