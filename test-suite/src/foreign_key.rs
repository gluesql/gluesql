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
        CREATE TABLE Parent (
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
            FOREIGN KEY(parent_id) REFERENCES Parent(id)
        );
    ",
    )
    .await;

    g.named_test(
        "If there is no parent, insert should fail",
        "INSERT INTO Child VALUES (1, 'foo', 1);",
        Err(ValidateError::ForeignKeyViolation {
            child_table: "Child".to_owned(),
            child_column: "parent_id".to_owned(),
            parent_table: "Parent".to_owned(),
            parent_column: "id".to_owned(),
        }
        .into()),
    )
    .await;
});
