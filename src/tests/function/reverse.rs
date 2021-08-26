use crate::*;

test_case!(reverse, async move {
    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES
                ("Let's meet")
            "#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT REVERSE(name) AS test FROM Item;",
            Ok(select!(
                "test"
                Value::Str;
                "teem s'teL".to_owned()
            )),
        ),
    ];
    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
