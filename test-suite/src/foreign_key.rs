use {
    crate::*,
    gluesql_core::{
        ast::{
            DataType::{Int, Text},
            ForeignKey, ReferentialAction,
        },
        error::{DeleteError, InsertError, TranslateError, UpdateError},
        executor::{AlterError, Referencing},
        prelude::Payload,
    },
};

test_case!(foreign_key, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE ReferencedTableWithoutPK (
            id INTEGER,
            name TEXT
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
            name TEXT
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
            name TEXT
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

    g.named_test(
        "Unsupported foreign key option: CASCADE",
        "CREATE TABLE ReferencingTable (
            id INT,
            name TEXT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE CASCADE
        );",
        Err(TranslateError::UnsupportedConstraint("CASCADE".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Unsupported foreign key option: SET DEFAULT",
        "CREATE TABLE ReferencingTable (
            id INT,
            name TEXT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT
        );",
        Err(TranslateError::UnsupportedConstraint("SET DEFAULT".to_owned()).into()),
    )
    .await;

    g.named_test(
        "Unsupported foreign key option: SET NULL",
        "CREATE TABLE ReferencingTable (
            id INT,
            name TEXT,
            referenced_id INT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL
        );",
        Err(TranslateError::UnsupportedConstraint("SET NULL".to_owned()).into()),
    )
    .await;

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
            name TEXT
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
        Ok(Payload::DropTable(1)),
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
        Ok(Payload::DropTable(1)),
    )
    .await;
});
