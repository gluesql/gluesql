//! Submodule for testing foreign key constraints on update.
//!
//! Since the Primary Keys cannot be updated at this time, operations such as `ON UPDATE CASCADE`,
//! `ON UPDATE SET NULL` or `ON UPDATE SET DEFAULT` are not supported. The only supported operation
//! is `ON UPDATE NO ACTION` or `ON UPDATE RESTRICT`. All other operations will result in a translation
//! error.
use {
    crate::*,
    gluesql_core::{
        error::{InsertError, TranslateError, UpdateError},
        prelude::Payload,
    },
};

test_case!(foreign_key_on_update, {
    let g = get_tester!();

    g.run(
        "CREATE TABLE Referenced (
            id INTEGER PRIMARY KEY,
        );",
    )
    .await;

    g.named_test(
        "On update cascade should fail as primary key cannot be updated",
        "CREATE TABLE Referencing (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced(id) ON UPDATE CASCADE
        );",
        Err(TranslateError::UnsupportedConstraint("ON UPDATE CASCADE".to_owned()).into()),
    )
    .await;

    g.named_test(
        "On update set null should fail as primary key cannot be updated",
        "CREATE TABLE Referencing (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced(id) ON UPDATE SET NULL
        );",
        Err(TranslateError::UnsupportedConstraint("ON UPDATE SET NULL".to_owned()).into()),
    )
    .await;

    g.named_test(
        "On update set default should fail as primary key cannot be updated",
        "CREATE TABLE Referencing (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced(id) ON UPDATE SET DEFAULT
        );",
        Err(TranslateError::UnsupportedConstraint("ON UPDATE SET DEFAULT".to_owned()).into()),
    )
    .await;

    g.named_test(
        "On update no action should pass as primary key cannot be updated",
        "CREATE TABLE Referencing1 (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced(id) ON UPDATE NO ACTION
        );",
        Ok(Payload::Create),
    )
    .await;

    g.named_test(
        "On update restrict should pass as primary key cannot be updated",
        "CREATE TABLE Referencing2 (
            id INTEGER PRIMARY KEY,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced(id) ON UPDATE RESTRICT
        );",
        Ok(Payload::Create),
    )
    .await;

    // We insert values in the Referenced table

    g.run("INSERT INTO Referenced (id) VALUES (1), (2), (3);")
        .await;

    // We insert values in the Referencing1 table

    g.run("INSERT INTO Referencing1 (id, referenced_id) VALUES (1, 1);")
        .await;

    // We insert values in the Referencing2 table

    g.run("INSERT INTO Referencing2 (id, referenced_id) VALUES (2, 2);")
        .await;

    // Updating the Referenced table should fail as it is trying to edit the primary key

    g.named_test(
        "Updating the Referenced table should fail as it is trying to edit the primary key",
        "UPDATE Referenced SET id = 4 WHERE id = 1;",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into()),
    )
    .await;

    // Updating the Referenced table should fail as it trying to edit the primary key

    g.named_test(
        "Updating the Referenced table should fail as it is trying to edit the primary key",
        "UPDATE Referenced SET id = 4 WHERE id = 2;",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into()),
    )
    .await;

    // Updating the Referenced table primary key even when it is not being referenced should fail
    // but because primary key update is not supported

    g.named_test(
        "Updating the Referenced table primary key even when it is not being referenced should fail",
        "UPDATE Referenced SET id = 4 WHERE id = 3;",
        Err(UpdateError::UpdateOnPrimaryKeyNotSupported("id".to_owned()).into()),
    ).await;

    // Trying to insert a value in the Referencing1 table that does not exist in the Referenced table
    // should fail

    g.named_test(
        "Trying to insert a value in the Referencing1 table that does not exist in the Referenced table should fail",
        "INSERT INTO Referencing1 (id, referenced_id) VALUES (2, 4);",
        Err(InsertError::CannotFindReferencedValue {
            table_name: "Referenced".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "4".to_owned(),
        }
        .into()),
    ).await;

    // Trying to update a value in the Referenced1 table that does not exist in the Referenced table
    // should fail

    g.named_test(
        "Trying to update a value in the Referenced1 table that does not exist in the Referenced table should fail",
        "UPDATE Referencing1 SET referenced_id = 4 WHERE id = 1;",
        Err(UpdateError::CannotFindReferencedValue {
            table_name: "Referenced".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "4".to_owned(),
        }
        .into()),
    ).await;

    // Trying to insert a value in the Referencing2 table that does not exist in the Referenced table
    // should fail

    g.named_test(
        "Trying to insert a value in the Referencing2 table that does not exist in the Referenced table should fail",
        "INSERT INTO Referencing2 (id, referenced_id) VALUES (3, 4);",
        Err(InsertError::CannotFindReferencedValue {
            table_name: "Referenced".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "4".to_owned(),
        }
        .into()),
    ).await;

    // Trying to update a value in the Referenced2 table that does not exist in the Referenced table
    // should fail

    g.named_test(
        "Trying to update a value in the Referenced2 table that does not exist in the Referenced table should fail",
        "UPDATE Referencing2 SET referenced_id = 4 WHERE id = 2;",
        Err(UpdateError::CannotFindReferencedValue {
            table_name: "Referenced".to_owned(),
            column_name: "id".to_owned(),
            referenced_value: "4".to_owned(),
        }
        .into()),
    ).await;
});
