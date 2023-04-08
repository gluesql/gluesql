use {
    crate::*,
    gluesql_core::{data::ValueError, executor::Payload, prelude::Value::Inet},
    std::{net::IpAddr, str::FromStr},
};

test_case!(inet, async move {
    let inet = |v| IpAddr::from_str(v).unwrap();

    let test_cases = [
        ("CREATE TABLE computer (ip INET)", Ok(Payload::Create)),
        (
            "INSERT INTO computer VALUES
                ('::1'),
                ('127.0.0.1'),
                ('0.0.0.0'),
                (4294967295),
                (9876543210);
            ",
            Ok(Payload::Insert(5)),
        ),
        (
            "SELECT * FROM computer",
            Ok(select!(
                ip
                Inet;
                inet("::1");
                inet("127.0.0.1");
                inet("0.0.0.0");
                inet("255.255.255.255");
                inet("::2:4cb0:16ea")
            )),
        ),
        (
            "SELECT * FROM computer WHERE ip > '127.0.0.1'",
            Ok(select!(
                ip
                Inet;
                inet("::1");
                inet("255.255.255.255");
                inet("::2:4cb0:16ea")
            )),
        ),
        (
            "SELECT * FROM computer WHERE ip = '127.0.0.1'",
            Ok(select!(
                ip
                Inet;
                inet("127.0.0.1")
            )),
        ),
        ("INSERT INTO computer VALUES (0)", Ok(Payload::Insert(1))),
        (
            r#"INSERT INTO computer VALUES ('127.0.0.0.1')"#,
            Err(ValueError::FailedToParseInetString("127.0.0.0.1".to_owned()).into()),
        ),
    ];

    for (sql, expected) in test_cases {
        test!(sql, expected);
    }
});
