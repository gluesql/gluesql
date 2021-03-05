use crate::*;

test_case!(auto_increment, async move {
    run!(
        r#"
        CREATE TABLE table (
            id INTEGER CONSTRAINT AUTO_INCREMENT PRIMARY KEY,
            name TEXT NOT NULL,
        )"#
    );
    run!(
        r#"
        INSERT INTO table (name) VALUES ('bleh'), ('blo'), ('blee')"#
    );
    let test_cases = vec![
        /*(
            r#"
        CREATE TABLE table (
            id INTEGER PRIMARY KEY AUTO_INCREMENT
        )"#,
            Err(CreateTableError::TableAlreadyExists.into()),
        ),*/
        /*(
            r#"
        CREATE TABLE table1 (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )"#,
            Err(CreateTableError::TableAlreadyExists.into()),
        ),*/
        (
            r#"
        CREATE TABLE table1 (
            id INTEGER PRIMARY KEY UNIQUE CONSTRAINT AUTO_INCREMENT NOT NULL
        )"#,
            Err(CreateTableError::TableAlreadyExists.into()),
        ),
    ];

    for (sql, expected) in test_cases.into_iter() {
        test!(expected, sql);
    }
});
