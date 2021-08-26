use crate::*;

test_case!(reverse, async move {
    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES
                ("Hi"), 
                ("I AM R"), 
                ("Let's meet"), 
            "#,
            Ok(Payload::Insert(3)),
        ),
        (
            "SELECT REVERSE(name) FROM Item;",
            Ok(select!(
                "REVERSE(name)"
                Value::Str;
                "iH".to_owned();
                "R MA I".to_owned();
                "teem s'teL".to_owned()
            )),
        ),
    ];
    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});