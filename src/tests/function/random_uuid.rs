use crate::*;

test_case!(random_uuid, async move {
    let test_cases = vec![
        ("CREATE TABLE SingleItem (id UUID)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (random_uuid())"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT random_uuid(0) as pi FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "RANDOM_UUID".to_owned(),
                expected: 0,
                found: 1,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }

    count!(1, "SELECT random_uuid() FROM SingleItem");
});
