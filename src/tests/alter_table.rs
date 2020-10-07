use crate::*;
use Value::I64;

pub fn alter_table(mut tester: impl tests::Tester) {
    tester.run_and_print("CREATE TABLE Foo (id INTEGER);");
    tester.run_and_print("INSERT INTO Foo VALUES (1), (2), (3);");

    let found = tester.run("SELECT id FROM Foo").expect("select");
    let expected = select!(I64; 1; 2; 3);
    assert_eq!(expected, found);

    tester.run_and_print("ALTER TABLE Foo RENAME TO Bar;");

    let found = tester.run("SELECT id FROM Bar").expect("select");
    let expected = select!(I64; 1; 2; 3);
    assert_eq!(expected, found);
}
