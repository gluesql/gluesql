use {
    crate::*,
    gluesql_core::{
        ast::{
            DataType::{Int, Text},
            ForeignKey, ReferentialAction,
        },
        error::{DeleteError, InsertError, UpdateError},
        executor::{AlterError, Referencing},
        prelude::{Payload, Value},
    },
};

test_case!(foreign_key_on_delete, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE ReferencedTableWithoutPK (
            id INTEGER,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be failed if referenced table does not exist",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES NonExistingTable (id) ON DELETE CASCADE
        );",
        Err(AlterError::ReferencedTableNotFound("NonExistingTable".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be failed if referenced table does not have primary key",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithoutPK (id)
        );",
        Err(AlterError::ReferencingNonPKColumn {
            referenced_table: "ReferencedTableWithoutPK".to_owned(),
            referenced_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    g.run(
        "CREATE TABLE ReferencedTableWithUnique (
            id INTEGER UNIQUE,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be failed if referenced table has only Unique constraint",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithUnique (id)
        );",
        Err(AlterError::ReferencingNonPKColumn {
            referenced_table: "ReferencedTableWithUnique".to_owned(),
            referenced_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    g.run(
        "CREATE TABLE ReferencedTableWithPK (
            id INTEGER PRIMARY KEY,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key on different data type should be failed",
        "CREATE TABLE ReferencingTable (
            id TEXT,
            referenced_id TEXT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        );",
        Err(AlterError::ForeignKeyDataTypeMismatch {
            referencing_column: "referenced_id".to_owned(),
            referencing_column_type: Text,
            referenced_column: "id".to_owned(),
            referenced_column_type: Int,
        }
        .into()),
    )
    .await;

    // We insert a row into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1), (2);")
        .await;

    // We create a table with a foreign key constraint that references another table
    // which includes a ON DELETE CASCADE clause.
    g.run(
        "CREATE TABLE ReferencingTableCascade (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE CASCADE
        );",
    )
    .await;

    // We re-insert the rows we just deleted in the referencing table.
    g.run("INSERT INTO ReferencingTableCascade VALUES (1, 1), (3, 2);")
        .await;

    // We check that there are now two rows in the referencing table.
    g.named_test(
        "Referencing table should have two rows",
        "SELECT * FROM ReferencingTableCascade;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![
                vec![Value::I64(1), Value::I64(1)],
                vec![Value::I64(3), Value::I64(2)],
            ],
        }),
    )
    .await;

    // We delete the row from the referenced table, which should yield a cascade delete
    // with three rows being deleted.
    g.named_test(
        "Deleting referenced row should delete referencing row as well",
        "DELETE FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Delete(2)),
    )
    .await;

    // We check that ReferencedTableWithPK still has one row.
    g.named_test(
        "Referenced table should have one row",
        "SELECT * FROM ReferencedTableWithPK;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![vec![Value::I64(2)]],
        }),
    )
    .await;

    // We check that the row from the referencing table has been deleted as well.
    g.named_test(
        "Referencing row should be deleted on delete cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![],
        }),
    )
    .await;

    // We check that the other row from the referencing table has NOT been deleted.
    g.named_test(
        "Referencing row should NOT be deleted on delete cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 3;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![vec![Value::I64(3), Value::I64(2)]],
        }),
    )
    .await;

    // We delete also the second row from the referenced table.
    g.named_test(
        "Deleting referenced row should delete referencing row as well",
        "DELETE FROM ReferencedTableWithPK WHERE id = 2;",
        Ok(Payload::Delete(2)),
    )
    .await;

    // We check it has been deleted from the referencing table.
    g.named_test(
        "Referencing row should be deleted",
        "SELECT * FROM ReferencingTableCascade WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![],
        }),
    )
    .await;

    // We check it has been deleted from the referenced table.
    g.named_test(
        "Referenced row should be deleted",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        }),
    )
    .await;

    // We drop the referencing table.
    g.run("DROP TABLE ReferencingTableCascade;").await;

    // Next, we proceed to test the ON DELETE SET NULL clause.
    // We start by creating the ReferencingTableSetNull table.

    g.run(
        "CREATE TABLE ReferencingTableSetNull (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL
        );",
    )
    .await;

    // We insert a row into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1), (2);")
        .await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetNull VALUES (1, 1), (2, 1), (3, 2);")
        .await;

    // We delete the row from the referenced table, which should result in the
    // referenced_id column of the referencing table being set to NULL.
    g.named_test(
        "Deleting referenced row should set referencing row's foreign key to NULL",
        "DELETE FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Delete(1)),
    )
    .await;

    // We check that the foreign key of the rows in the referencing table has been set to NULL.
    g.named_test(
        "Referencing row's foreign key should be set to NULL",
        "SELECT * FROM ReferencingTableSetNull;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![
                vec![Value::I64(1), Value::Null],
                vec![Value::I64(2), Value::Null],
                vec![Value::I64(3), Value::I64(2)],
            ],
        }),
    )
    .await;

    // We check that the row has been deleted from the referenced table.
    g.named_test(
        "Referenced row should be deleted",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned()],
            rows: vec![],
        }),
    )
    .await;

    // We drop the referencing table.
    g.run("DROP TABLE ReferencingTableSetNull;").await;

    // Next, we proceed to test the ON DELETE SET DEFAULT clause.
    // We start by creating the ReferencingTableSetDefault table.

    g.run(
        "CREATE TABLE ReferencingTableSetDefault (
            id INT,
            referenced_id INT DEFAULT 1,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT
        );",
    )
    .await;

    // We insert a couple rows into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1), (3);")
        .await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetDefault VALUES (1, 2), (2, 3), (3, 3);")
        .await;

    // We delete the row from the referenced table with ID 2, which should result in the
    // referenced_id column of the rows in the referencing table being set to 1.
    g.named_test(
        "Deleting referenced row should set referencing row's foreign key to DEFAULT",
        "DELETE FROM ReferencedTableWithPK WHERE id = 2;",
        Ok(Payload::Delete(1)),
    )
    .await;

    // We check that the foreign key of the rows in the referencing table has been set to 1.
    g.named_test(
        "Referencing row's foreign key should be set to DEFAULT on delete cascade",
        "SELECT * FROM ReferencingTableSetDefault ORDER BY id ASC;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "referenced_id".to_owned()],
            rows: vec![
                vec![Value::I64(1), Value::I64(1)],
                vec![Value::I64(2), Value::I64(3)],
                vec![Value::I64(3), Value::I64(3)],
            ],
        }),
    )
    .await;

    // We drop the referencing table.
    g.run("DROP TABLE ReferencingTableSetDefault;").await;

    // We truncate the referenced table.
    g.run("DELETE FROM ReferencedTableWithPK;").await;

    g.named_test(
        "Referencing column not found",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            FOREIGN KEY (wrong_referencing_column) REFERENCES ReferencedTableWithPK (id)
        );",
        Err(AlterError::ReferencingColumnNotFound("wrong_referencing_column".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Referenced column not found",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (wrong_referenced_column)
        );",
        Err(AlterError::ReferencedColumnNotFound("wrong_referenced_column".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be succeeded if referenced table has primary key. NO ACTION(=RESTRICT) is default",
        "CREATE TABLE ReferencingTable (
            id INT,
            referenced_id INT,
            CONSTRAINT MyFkConstraint FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE NO ACTION ON UPDATE RESTRICT
        );",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "If there is no referenced value, insert should fail",
        "INSERT INTO ReferencingTable VALUES (1, 1);",
        Err(InsertError::CannotFindReferencedValue {
            table_name: "ReferencedTableWithPK".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "1".to_owned(),
        }
        .into()),
    )
    .await;

    g.named_test(
        "Even If there is no referenced value, NULL should be inserted",
        "INSERT INTO ReferencingTable VALUES (1, NULL);",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.run("INSERT INTO ReferencedTableWithPK VALUES (1);").await;

    g.named_test(
        "With valid referenced value, insert should succeed",
        "INSERT INTO ReferencingTable VALUES (2, 1);",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.named_test(
        "If there is no referenced value, update should fail",
        "UPDATE ReferencingTable SET referenced_id = 2 WHERE id = 2;",
        Err(UpdateError::CannotFindReferencedValue {
            table_name: "ReferencedTableWithPK".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "2".to_owned(),
        }
        .into()),
    )
    .await;

    g.named_test(
        "Even If there is no referenced value, it should be able to update to NULL",
        "UPDATE ReferencingTable SET referenced_id = NULL WHERE id = 2;",
        Ok(Payload::Update(1)),
    )
    .await;

    g.named_test(
        "With valid referenced value, update should succeed",
        "UPDATE ReferencingTable SET referenced_id = 1 WHERE id = 2;",
        Ok(Payload::Update(1)),
    )
    .await;

    g.named_test(
        "Deleting referenced row should fail if referencing value exists (by default: NO ACTION and gets error)",
        "DELETE FROM ReferencedTableWithPK WHERE id = 1;",
        Err(DeleteError::ReferencingColumnExists("ReferencingTable.referenced_id".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Deleting referencing table does not care referenced table",
        "DELETE FROM ReferencingTable WHERE id = 2;",
        Ok(Payload::Delete(1)),
    )
    .await;

    g.run(
        "
        CREATE TABLE ReferencedTableWithPK_2 (
            id INTEGER PRIMARY KEY,
        );",
    )
    .await;

    g.run("INSERT INTO ReferencedTableWithPK_2 VALUES (1);")
        .await;

    g.named_test(
        "Table with two foreign keys",
        "CREATE TABLE ReferencingWithTwoFK (
            id INTEGER PRIMARY KEY,
            referenced_id_1 INTEGER,
            referenced_id_2 INTEGER,
            FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
            FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
        );",
        Ok(Payload::Create),
    )
    .await;

    g.run("INSERT INTO ReferencingWithTwoFK VALUES (1, 1, 1);")
        .await;

    g.named_test(
        "Cannot update referenced_id_2 if there is no referenced value",
        "UPDATE ReferencingWithTwoFK SET referenced_id_2 = 9 WHERE id = 1;",
        Err(UpdateError::CannotFindReferencedValue {
            table_name: "ReferencedTableWithPK_2".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "9".to_owned(),
        }
        .into()),
    )
    .await;

    g.named_test(
        "Cannot drop referenced table if referencing table exists",
        "DROP TABLE ReferencedTableWithPK;",
        Err(AlterError::CannotDropTableWithReferencing {
            referenced_table_name: "ReferencedTableWithPK".to_owned(),
            referencings: vec![
                Referencing {
                    table_name: "ReferencingTable".to_owned(),
                    foreign_key: ForeignKey {
                        name: "MyFkConstraint".to_owned(),
                        referencing_column_name: "referenced_id".to_owned(),
                        referenced_table_name: "ReferencedTableWithPK".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
                Referencing {
                    table_name: "ReferencingWithTwoFK".to_owned(),
                    foreign_key: ForeignKey {
                        name: "FK_referenced_id_1-ReferencedTableWithPK_id".to_owned(),
                        referencing_column_name: "referenced_id_1".to_owned(),
                        referenced_table_name: "ReferencedTableWithPK".to_owned(),
                        referenced_column_name: "id".to_owned(),
                        on_delete: ReferentialAction::NoAction,
                        on_update: ReferentialAction::NoAction,
                    },
                },
            ],
        }
        .into()),
    )
    .await;

    g.named_test(
        "Dropping table with cascade should drop both table and constraint",
        "DROP TABLE ReferencedTableWithPK CASCADE;",
        Ok(Payload::DropTable),
    )
    .await;

    g.named_test(
        "Should create self referencing table",
        "CREATE TABLE SelfReferencingTable (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
        );",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "Dropping self referencing table should succeed",
        "DROP TABLE SelfReferencingTable;",
        Ok(Payload::DropTable),
    )
    .await;
});
