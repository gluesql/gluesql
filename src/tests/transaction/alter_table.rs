#![cfg(all(feature = "transaction", feature = "alter-table"))]

use {crate::*, prelude::Value::*, store::AlterTableError};

test_case!(alter_table_rename_table, async move {
    run!("CREATE TABLE RenameTable (id INTEGER);");
    run!("INSERT INTO RenameTable VALUES (1);");
    run!("BEGIN;");
    run!("ALTER TABLE RenameTable RENAME TO NewName;");
    test!(
        Err(AlterTableError::TableNotFound("RenameTable".to_owned()).into()),
        "SELECT * FROM RenameTable"
    );
    test!(Ok(select!(id I64; 1)), "SELECT * FROM NewName");
    run!("ROLLBACK;");
    test!(
        Err(AlterTableError::TableNotFound("NewName".to_owned()).into()),
        "SELECT * FROM NewName"
    );
    test!(Ok(select!(id I64; 1)), "SELECT * FROM RenameTable");
});

test_case!(alter_table_rename_column, async move {
    run!("CREATE TABLE RenameCol (id INTEGER);");
    run!("INSERT INTO RenameCol VALUES (1);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    test!(Ok(select!(new_id I64; 1)), "SELECT * FROM RenameCol");
    run!("ROLLBACK;");
    test!(Ok(select!(id I64; 1)), "SELECT * FROM RenameCol");

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    run!("COMMIT;");
    test!(Ok(select!(new_id I64; 1)), "SELECT * FROM RenameCol");
});

test_case!(alter_table_add_column, async move {
    run!("CREATE TABLE AddCol (id INTEGER);");
    run!("INSERT INTO AddCol VALUES (1);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    test!(
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
        "SELECT * FROM AddCol"
    );
    run!("ROLLBACK;");
    test!(Ok(select!(id I64; 1)), "SELECT * FROM AddCol");

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    run!("COMMIT;");
    test!(
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        )),
        "SELECT * FROM AddCol"
    );
});

test_case!(alter_table_drop_column, async move {
    run!("CREATE TABLE DropCol (id INTEGER, num INTEGER);");
    run!("INSERT INTO DropCol VALUES (1, 2);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE DropCol DROP COLUMN num;");
    test!(Ok(select!(id I64; 1)), "SELECT * FROM DropCol");
    run!("ROLLBACK;");
    test!(
        Ok(select!(
            id  | num
            I64 | I64;
            1     2
        )),
        "SELECT * FROM DropCol"
    );

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE DropCol DROP COLUMN num;");
    run!("COMMIT;");
    test!(Ok(select!(id I64; 1)), "SELECT * FROM DropCol");
});
