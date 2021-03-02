use crate::*;

test_case!(left_right, async move {
    use Value::Str;
    let test_cases = vec![
        ("CREATE TABLE Item (name TEXT)", Ok(Payload::Create)),
        (
            r#"INSERT INTO Item VALUES ("Blop mc blee"), ("B"), ("Steven the &long named$ folken!")"#,
            Ok(Payload::Insert(3)),
        ),
        (
            r#"SELECT LEFT(name, 3) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "Blo".to_owned();
                "B".to_owned();
                "Ste".to_owned()
            )),
        ),
        (
            r#"SELECT RIGHT(name, 10) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "op mc blee".to_owned();
                "B".to_owned();
                "d$ folken!".to_owned()
            )),
        ),
        /*( Concatenation not currently supported
            r#"SELECT LEFT((name + 'bobbert'), 10) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "Blop mc blee".to_owned();
                "Bbobbert".to_owned();
                "Steven the".to_owned()
            )),
        ),*/
        (
            r#"SELECT LEFT('blue', 10) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "blue".to_owned();
                "blue".to_owned();
                "blue".to_owned()
            )),
        ),
        (
            r#"SELECT LEFT("blunder", 3) AS test FROM Item"#,
            Ok(select!(
                "test"
                Str;
                "blu".to_owned();
                "blu".to_owned();
                "blu".to_owned()
            )),
        ),
    ];
    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
