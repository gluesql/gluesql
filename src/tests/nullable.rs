use crate::*;

pub fn nullable(mut tester: impl tests::Tester) {
    tester.run_and_print(
        r#"
CREATE TABLE Test (
    id INTEGER NULL,
    num INTEGER,
    name TEXT
)"#,
    );
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (NULL, 2, \"Hello\")");
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")");

    use Value::*;

    let found = tester
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        OptI64  I64 Str;
        None    2   "Hello".to_owned();
        Some(1) 9   "World".to_owned();
        Some(3) 4   "Great".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE id = NULL AND name = \'Hello\'")
        .expect("select");
    let expected = select!(OptI64 I64; None 2);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE id IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE id IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE id + 1 IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE id + 1 IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE 100 IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE 100 IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE 8 + 3 IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE 8 + 3 IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE NULL IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE NULL IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE \"NULL\" IS NULL")
        .expect("select");
    let expected = select!(OptI64 I64);
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, num FROM Test WHERE \"NULL\" IS NOT NULL")
        .expect("select");
    let expected = select!(OptI64 I64; None 2; Some(1) 9; Some(3) 4);
    assert_eq!(expected, found);

    tester.run_and_print("UPDATE Test SET id = 2");

    let found = tester.run("SELECT id FROM Test").expect("select");
    let expected = select!(OptI64; Some(2); Some(2); Some(2));
    assert_eq!(expected, found);

    let found = tester.run("SELECT id, num FROM Test").expect("select");
    let expected = select!(OptI64 I64; Some(2) 2; Some(2) 9; Some(2) 4);
    assert_eq!(expected, found);

    let found = tester.run("INSERT INTO Test VALUES (1, NULL)");
    let expected = Err(ValueError::NullValueOnNotNullField.into());
    assert_eq!(expected, found);
}

pub fn nullable_text(mut tester: impl tests::Tester) {
    tester.run_and_print(
        "
        CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        );
    ",
    );

    let insert_sqls = [
        "INSERT INTO Foo (id, name) VALUES (1, \"Hello\")",
        "INSERT INTO Foo (id, name) VALUES (2, Null)",
    ];

    for insert_sql in insert_sqls.iter() {
        tester.run(insert_sql).unwrap();
    }
}
