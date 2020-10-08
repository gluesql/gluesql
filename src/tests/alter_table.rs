use crate::*;
use Value::I64;

pub fn alter_table(mut tester: impl tests::Tester) {
    tester.run_and_print("CREATE TABLE Foo (id INTEGER);");
    tester.run_and_print("INSERT INTO Foo VALUES (1), (2), (3);");

    assert_eq!(
        select!(I64; 1; 2; 3),
        tester.run("SELECT id FROM Foo").expect("select"),
    );

    tester.run_and_print("ALTER TABLE Foo RENAME TO Bar;");

    assert_eq!(
        select!(I64; 1; 2; 3),
        tester.run("SELECT id FROM Bar").expect("select"),
    );

    tester.run_and_print("ALTER TABLE Bar RENAME COLUMN id TO new_id");

    assert_eq!(
        select!(I64; 1; 2; 3),
        tester.run("SELECT new_id FROM Bar").expect("select"),
    );

    assert_eq!(
        select!(I64; 1; 2; 3),
        tester.run("SELECT new_id FROM Bar").expect("select"),
    );

    assert_eq!(
        Err(StoreError::ColumnNotFound.into()),
        tester.run("ALTER TABLE Bar RENAME COLUMN hello TO idid"),
    );
}
