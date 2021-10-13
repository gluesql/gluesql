use crate::*;
use test::*;

test_case!(pi, async move {
    use Value::F64;

    let test_cases = vec![
        ("CREATE TABLE SingleItem (id FLOAT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO SingleItem VALUES (0)"#,
            Ok(Payload::Insert(1)),
        ),
        (
            "SELECT PI() as pi FROM SingleItem",
            Ok(select!(
                pi
                F64;
                std::f64::consts::PI
            )),
        ),
        (
            "SELECT PI(0) as pi FROM SingleItem",
            Err(TranslateError::FunctionArgsLengthNotMatching {
                name: "PI".to_owned(),
                expected: 0,
                found: 1,
            }
            .into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(expected, sql);
    }
});
