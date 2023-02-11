use {
    crate::*,
    gluesql_core::{executor::FetchError, prelude::Value::*},
};

test_case!(alter_table_rename_table, async move {
    run!("CREATE TABLE RenameTable (id INTEGER);");
    run!("INSERT INTO RenameTable VALUES (1);");
    run!("BEGIN;");
    run!("ALTER TABLE RenameTable RENAME TO NewName;");
    test!(
        "SELECT * FROM RenameTable",
        Err(FetchError::TableNotFound("RenameTable".to_owned()).into())
    );
    test!("SELECT * FROM NewName", Ok(select!(id I64; 1)));
    run!("ROLLBACK;");
    test!(
        "SELECT * FROM NewName",
        Err(FetchError::TableNotFound("NewName".to_owned()).into())
    );
    test!("SELECT * FROM RenameTable", Ok(select!(id I64; 1)));
});

test_case!(alter_table_rename_column, async move {
    run!("CREATE TABLE RenameCol (id INTEGER);");
    run!("INSERT INTO RenameCol VALUES (1);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    test!("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)));
    run!("ROLLBACK;");
    test!("SELECT * FROM RenameCol", Ok(select!(id I64; 1)));

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE RenameCol RENAME COLUMN id TO new_id;");
    run!("COMMIT;");
    test!("SELECT * FROM RenameCol", Ok(select!(new_id I64; 1)));
});

test_case!(alter_table_add_column, async move {
    run!("CREATE TABLE AddCol (id INTEGER);");
    run!("INSERT INTO AddCol VALUES (1);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    test!(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        ))
    );
    run!("ROLLBACK;");
    test!("SELECT * FROM AddCol", Ok(select!(id I64; 1)));

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE AddCol ADD COLUMN new_col INTEGER DEFAULT 3;");
    run!("COMMIT;");
    test!(
        "SELECT * FROM AddCol",
        Ok(select!(
            id  | new_col
            I64 | I64;
            1     3
        ))
    );
});

test_case!(alter_table_drop_column, async move {
    run!("CREATE TABLE DropCol (id INTEGER, num INTEGER);");
    run!("INSERT INTO DropCol VALUES (1, 2);");

    // ROLLBACK
    run!("BEGIN;");
    run!("ALTER TABLE DropCol DROP COLUMN num;");
    test!("SELECT * FROM DropCol", Ok(select!(id I64; 1)));
    run!("ROLLBACK;");
    test!(
        "SELECT * FROM DropCol",
        Ok(select!(
            id  | num
            I64 | I64;
            1     2
        ))
    );

    // COMMIT
    run!("BEGIN;");
    run!("ALTER TABLE DropCol DROP COLUMN num;");
    run!("COMMIT;");
    test!("SELECT * FROM DropCol", Ok(select!(id I64; 1)));
});
