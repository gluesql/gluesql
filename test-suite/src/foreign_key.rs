use gluesql_core::executor::{AlterError, ExecuteError};

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
        "Create table with foreign key should be failed if parent table does not have primary key",
        "
        CREATE TABLE Child (
            id INT,
            name TEXT,
            parent_id INT,
            FOREIGN KEY(parent_id) REFERENCES ParentWithoutPK(id)
        );
        ",
        Err(AlterError::ReferredColumnNotUnique {
            foreign_table: "ParentWithoutPK".to_owned(),
            referred_column: "id".to_owned(),
        }
        .into()),
    )
    .await;

    // g.run(
    //     "
    //     CREATE TABLE Parent (
    //         id INTEGER PRIMARY KEY,
    //         name TEXT,
    //     );
    // ",
    // )
    // .await;

    // // should be error if Parent does not have PK or unique
    // g.run(
    //     "
    //     CREATE TABLE Child (
    //         id INT,
    //         name TEXT,
    //         parent_id INT,
    //         FOREIGN KEY(parent_id) REFERENCES Parent(id)
    //     );
    // ",
    // )
    // .await;

    // g.named_test(
    //     "If there is no parent, insert should fail",
    //     "INSERT INTO Child VALUES (1, 'foo', 1);",
    //     Err(ValidateError::ForeignKeyViolation {
    //         name: "aaa".to_owned(),
    //         table: "Child".to_owned(),
    //         column: "parent_id".to_owned(),
    //         foreign_table: "Parent".to_owned(),
    //         referred_column: "id".to_owned(),
    //     }
    //     .into()),
    // )
    // .await;
});
