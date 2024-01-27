use gluesql_core::executor::{AlterError, ExecuteError, ReferingChild};

use {
    crate::*,
    gluesql_core::{
        data::Value::*,
        error::{UpdateError, ValidateError, ValueError},
        prelude::{Key, Payload},
    },
};

test_case!(foreign_key, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE ParentWithoutPK (
            id INTEGER,
            name TEXT,
        );
    ",
    )
    .await;

    g.named_test(
        "Create table with foreign key should be failed if parent table does not have primary key or unique",
        "
        CREATE TABLE Child (
            id INT, name TEXT,
            parent_id INT,
            FOREIGN KEY(parent_id) REFERENCES ParentWithoutPK(id)
        );
        ",
        Err(AlterError::ReferredColumnNotUnique {
            referred_table: "ParentWithoutPK".to_owned(),
            referred_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    g.run(
        "
        CREATE TABLE ParentWithUnique (
            id INTEGER UNIQUE,
            name TEXT,
        );
    ",
    )
    .await;

    g.run(
        "
        CREATE TABLE ChildReferringUnique (
            id INT,
            name TEXT,
            parent_id INT,
            FOREIGN KEY(parent_id) REFERENCES ParentWithUnique(id)
        );
    ",
    )
    .await;

    g.run(
        "
        CREATE TABLE ParentWithPK (
            id INTEGER PRIMARY KEY,
            name TEXT,
        );
    ",
    )
    .await;

    g.run(
        "
        CREATE TABLE Child (
            id INT,
            name TEXT,
            parent_id INT,
            FOREIGN KEY(parent_id) REFERENCES ParentWithPK(id)
        );
    ",
    )
    .await;

    g.named_test(
        "If there is no parent, insert should fail",
        "INSERT INTO Child VALUES (1, 'orphan', 1);",
        Err(ValidateError::ForeignKeyViolation {
            name: "FK_Child_parent_id-ParentWithPK_id".to_owned(),
            table: "Child".to_owned(),
            column: "parent_id".to_owned(),
            referred_table: "ParentWithPK".to_owned(),
            referred_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    g.named_test(
        "Even If there is no parent, NULL should be inserted",
        "INSERT INTO Child VALUES (1, 'Null is independent', NULL);",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.run("INSERT INTO ParentWithPK VALUES (1, 'parent1');")
        .await;

    g.named_test(
        "With valid parent, insert should succeed",
        "INSERT INTO Child VALUES (2, 'child with parent', 1);",
        Ok(Payload::Insert(1)),
    )
    .await;

    g.named_test(
        "If there is no parent, update should fail",
        "UPDATE Child SET parent_id = 2 WHERE id = 2;",
        Err(ValidateError::ForeignKeyViolation {
            name: "FK_Child_parent_id-ParentWithPK_id".to_owned(),
            table: "Child".to_owned(),
            column: "parent_id".to_owned(),
            referred_table: "ParentWithPK".to_owned(),
            referred_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    g.named_test(
        "Even If there is no parent, it should be able to update to NULL",
        "UPDATE Child SET parent_id = NULL WHERE id = 2;",
        Ok(Payload::Update(1)),
    )
    .await;

    g.named_test(
        "With valid parent, update should succeed",
        "UPDATE Child SET parent_id = 1 WHERE id = 2;",
        Ok(Payload::Update(1)),
    )
    .await;

    g.named_test(
        "Deleting child does not care parents",
        "DELETE FROM Child WHERE id = 2;",
        Ok(Payload::Delete(1)),
    )
    .await;

    g.named_test(
        "Cannot drop parent if child exists",
        "DROP TABLE ParentWithPK;",
        Err(AlterError::CannotDropTableParentOnReferringChildren {
            parent: "ParentWithPK".to_owned(),
            referring_children: vec![ReferingChild {
                table_name: "Child".to_owned(),
                constraint_name: "FK_Child_parent_id-ParentWithPK_id".to_owned(),
            }],
        }
        .into()),
    )
    .await;
});
