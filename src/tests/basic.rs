use crate::*;

pub fn basic(mut tester: impl tests::Tester) {
    tester.run_and_print(
        r#"
CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
)"#,
    );
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 2, \"Hello\")");
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (1, 9, \"World\")");
    tester.run_and_print("INSERT INTO Test (id, num, name) VALUES (3, 4, \"Great\")");

    use Value::*;

    let found = tester
        .run("SELECT id, num, name FROM Test")
        .expect("select");
    let expected = select!(
        I64 I64 Str;
        1   2   "Hello".to_owned();
        1   9   "World".to_owned();
        3   4   "Great".to_owned()
    );
    assert_eq!(expected, found);

    tester.run_and_print("UPDATE Test SET id = 2");

    let found = tester.run("SELECT id FROM Test").expect("select");
    let expected = select!(I64; 2; 2; 2);
    assert_eq!(expected, found);

    let found = tester.run("SELECT id, num FROM Test").expect("select");
    let expected = select!(I64 I64; 2 2; 2 9; 2 4);
    assert_eq!(expected, found);
}
