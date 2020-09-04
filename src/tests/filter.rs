use crate::*;

pub fn between(mut tester: impl tests::Tester) {
    tester.run_and_print(
        r#"
CREATE TABLE Test (
    id INTEGER,
    name TEXT,
    strength FLOAT
)"#,
    );
    tester.run_and_print("INSERT INTO Test (id, name, strength) VALUES (1, \"Amelia\", 10.10)");
    tester.run_and_print("INSERT INTO Test (id, name, strength) VALUES (2, \"Doll\", 20.20)");
    tester.run_and_print("INSERT INTO Test (id, name, strength) VALUES (3, \"Gascoigne\", 30.30)");
    tester.run_and_print("INSERT INTO Test (id, name, strength) VALUES (4, \"Gehrman\", 40.40)");
    tester.run_and_print("INSERT INTO Test (id, name, strength) VALUES (5, \"Maria\", 50.50)");

    use Value::*;

    let found = tester
        .run("SELECT id, name FROM Test WHERE id BETWEEN 2 AND 4")
        .expect("select");
    let expected = select!(
        I64 Str;
        2   "Doll".to_owned();
        3   "Gascoigne".to_owned();
        4   "Gehrman".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT id, name FROM Test WHERE name BETWEEN 'Doll' AND 'Gehrman'")
        .expect("select");
    let expected = select!(
        I64 Str;
        2   "Doll".to_owned();
        3   "Gascoigne".to_owned();
        4   "Gehrman".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT name FROM Test WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'")
        .expect("select");
    let expected = select!(
        Str;
        "Amelia".to_owned();
        "Maria".to_owned()
    );
    assert_eq!(expected, found);

    let found = tester
        .run("SELECT strength, name FROM Test WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'")
        .expect("select");
    let expected = select!(
        F64   Str;
        10.10 "Amelia".to_owned();
        50.50 "Maria".to_owned()
    );
    assert_eq!(expected, found);
}
