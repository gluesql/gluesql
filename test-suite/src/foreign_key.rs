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

test_case!(foreign_key, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE ReferencedTableWithoutPK (
            id INTEGER,
            name TEXT,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be failed if referenced table does not have primary key",
        "CREATE TABLE ReferencingTable (
            id INT, name TEXT,
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
            name TEXT,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key should be failed if referenced table has only Unique constraint",
        "CREATE TABLE ReferencingTable (
            id INT,
            name TEXT,
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
            name TEXT,
        );",
    )
    .await;

    g.named_test(
        "Creating table with foreign key on different data type should be failed",
        "CREATE TABLE ReferencingTable (
            id TEXT,
            name TEXT,
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

    // We create a table with a foreign key constraint that references another table
    // which includes a ON DELETE CASCADE clause.
    g.run(
        "CREATE TABLE ReferencingTableCascade (
            id INT,
            name TEXT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON UPDATE CASCADE ON DELETE CASCADE
        );",
    )
    .await;

    // We insert a row into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1'), (2, 'referenced_table2');")
        .await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableCascade VALUES (1, 'referencing_table with referenced_table', 1), (2, 'referencing_table with referenced_table', 2), (3, 'referencing_table with referenced_table', 1);")
        .await;

    // We update the row in the referenced table, which should yield a cascade update
    // with the one row in the referencing table being updated, and the other rows from
    // the referencing table being deleted.
    g.named_test(
        "Updating referenced row should update referencing row as well",
        "UPDATE ReferencedTableWithPK SET name = 'referenced_table1 updated' WHERE id = 1;",
        Ok(Payload::Update(1)),
    )
    .await;

    // We check that the row from the referencing table has been deleted as well.
    g.named_test(
        "Referencing row should be deleted on update cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![],
        }),
    )
    .await;

    // We check that the other row from the referencing table has NOT been deleted.
    g.named_test(
        "Referencing row should NOT be deleted on update cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![vec![
                Value::I64(2),
                Value::Str("referencing_table with referenced_table".to_owned()),
                Value::I64(2),
            ]],
        }),
    )
    .await;

    // We check that the row in the referenced table has been updated.
    g.named_test(
        "Referenced row should be updated",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![vec![
                Value::I64(1),
                Value::Str("referenced_table1 updated".to_owned()),
            ]],
        }),
    )
    .await;

    // We re-insert the rows we just deleted in the referencing table.
    g.run("INSERT INTO ReferencingTableCascade VALUES (1, 'referencing_table with referenced_table', 1), (3, 'referencing_table with referenced_table', 1);")
        .await;

    // We delete the row from the referenced table, which should yield a cascade delete
    // with three rows being deleted.
    g.named_test(
        "Deleting referenced row should delete referencing row as well",
        "DELETE FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Delete(3)),
    )
    .await;

    // We check that the row from the referencing table has been deleted as well.
    g.named_test(
        "Referencing row should be deleted on delete cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![],
        }),
    )
    .await;

    // We check that the other row from the referencing table has NOT been deleted.
    g.named_test(
        "Referencing row should NOT be deleted on delete cascade",
        "SELECT * FROM ReferencingTableCascade WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![vec![
                Value::I64(2),
                Value::Str("referencing_table with referenced_table".to_owned()),
                Value::I64(2),
            ]],
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
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![],
        }),
    )
    .await;

    // We check it has been deleted from the referenced table.
    g.named_test(
        "Referenced row should be deleted",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
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
            name TEXT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL ON UPDATE SET NULL
        );",
    )
    .await;

    // We insert a row into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1');")
        .await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetNull VALUES (1, 'referencing_table with referenced_table', 1), (2, 'referencing_table with referenced_table', 1);")
        .await;

    // We update the row in the referenced table, which should result in the
    // referenced_id column of the rows in the referencing table being set to NULL.
    g.named_test(
        "Updating referenced row should set referencing row's foreign key to NULL on update",
        "UPDATE ReferencedTableWithPK SET name = 'referenced_table1 updated' WHERE id = 1;",
        Ok(Payload::Update(3)),
    )
    .await;

    // We check that the foreign key of the rows in the referencing table has been set to NULL.
    g.named_test(
        "Referencing row's foreign key should be set to NULL",
        "SELECT * FROM ReferencingTableSetNull;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::Null,
                ],
                vec![
                    Value::I64(2),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::Null,
                ],
            ],
        }),
    )
    .await;

    // We check that the row has been updated from the referenced table.
    g.named_test(
        "Referenced row should be updated",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![vec![
                Value::I64(1),
                Value::Str("referenced_table1 updated".to_owned()),
            ]],
        }),
    )
    .await;

    // We truncate the referencing table content.
    g.run("DELETE FROM ReferencingTableSetNull;").await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetNull VALUES (1, 'referencing_table with referenced_table', 1), (2, 'referencing_table with referenced_table', 1);")
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
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::Null,
                ],
                vec![
                    Value::I64(2),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::Null,
                ],
            ],
        }),
    )
    .await;

    // We check that the row has been deleted from the referenced table.
    g.named_test(
        "Referenced row should be deleted",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 1;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
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
            name TEXT,
            referenced_id INT DEFAULT 1,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT
        );",
    )
    .await;

    // We insert a couple rows into the referenced table.
    g.run("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1'), (2, 'referenced_table2'), (3, 'referenced_table3');")
        .await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetDefault VALUES (1, 'referencing_table with referenced_table', 2), (2, 'referencing_table with referenced_table', 3), (3, 'referencing_table with referenced_table', 3);")
        .await;

    // We update the row in the referenced table with ID 3, which should result in the
    // referenced_id column of the rows in the referencing table being set to 1.
    g.named_test(
        "Updating referenced row should set referencing row's foreign key to DEFAULT on update",
        "UPDATE ReferencedTableWithPK SET name = 'referenced_table3 updated' WHERE id = 2;",
        Ok(Payload::Update(2)),
    )
    .await;

    // We check that the foreign key of the rows in the referencing table has been set to 1.
    g.named_test(
        "Referencing row's foreign key should be set to DEFAULT on update cascade",
        "SELECT * FROM ReferencingTableSetDefault ORDER BY id ASC;",
        Ok(Payload::Select {
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(1),
                ],
                vec![
                    Value::I64(2),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(3),
                ],
                vec![
                    Value::I64(3),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(3),
                ],
            ],
        }),
    )
    .await;

    // We check that the row has been updated from the referenced table.
    g.named_test(
        "Referenced row should be updated",
        "SELECT * FROM ReferencedTableWithPK WHERE id = 2;",
        Ok(Payload::Select {
            labels: vec!["id".to_owned(), "name".to_owned()],
            rows: vec![vec![
                Value::I64(2),
                Value::Str("referenced_table3 updated".to_owned()),
            ]],
        }),
    )
    .await;

    // We truncate the referencing table content.
    g.run("DELETE FROM ReferencingTableSetDefault;").await;

    // We insert a row into the referencing table.
    g.run("INSERT INTO ReferencingTableSetDefault VALUES (1, 'referencing_table with referenced_table', 2), (2, 'referencing_table with referenced_table', 3), (3, 'referencing_table with referenced_table', 3);")
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
            labels: vec![
                "id".to_owned(),
                "name".to_owned(),
                "referenced_id".to_owned(),
            ],
            rows: vec![
                vec![
                    Value::I64(1),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(1),
                ],
                vec![
                    Value::I64(2),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(3),
                ],
                vec![
                    Value::I64(3),
                    Value::Str("referencing_table with referenced_table".to_owned()),
                    Value::I64(3),
                ],
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
            name TEXT,
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
            name TEXT,
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
            name TEXT,
            referenced_id INT,
            CONSTRAINT MyFkConstraint FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE NO ACTION ON UPDATE RESTRICT
        );",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "If there is no referenced value, insert should fail",
        "INSERT INTO ReferencingTable VALUES (1, 'orphan', 1);",
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
        "INSERT INTO ReferencingTable VALUES (1, 'Null is independent', NULL);",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.run("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1');")
        .await;

    g.named_test(
        "With valid referenced value, insert should succeed",
        "INSERT INTO ReferencingTable VALUES (2, 'referencing_table with referenced_table', 1);",
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
        Err(DeleteError::RestrictingColumnExists("ReferencingTable.referenced_id".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Updating referenced row should fail if referencing value exists (by default: NO ACTION and gets error)",
        "UPDATE ReferencedTableWithPK SET name = 'referenced_table1 updated' WHERE id = 1;",
        Err(UpdateError::RestrictingColumnExists("ReferencingTable.referenced_id".to_owned()).into()),
    ).await;

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
            name TEXT,
        );",
    )
    .await;

    g.run("INSERT INTO ReferencedTableWithPK_2 VALUES (1, 'referenced_table2');")
        .await;

    g.named_test(
        "Table with two foreign keys",
        "CREATE TABLE ReferencingWithTwoFK (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id_1 INTEGER,
            referenced_id_2 INTEGER,
            FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
            FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
        );",
        Ok(Payload::Create),
    )
    .await;

    g.run(
        "INSERT INTO ReferencingWithTwoFK VALUES (1, 'referencing_table with two referenced_table', 1, 1);"
    ).await;

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
            name TEXT,
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
