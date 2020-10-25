use crate::*;

pub fn create_table(mut tester: impl tests::Tester) {
    let mut run = |sql| tester.run(sql);

    let test_cases = vec![
        (
            r#"
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE CreateTable1 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Err(ExecuteError::TableAlreadyExists.into()),
        ),
        (
            r#"
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
        (
            r#"
        CREATE TABLE IF NOT EXISTS CreateTable2 (
            id INTEGER NULL,
            num INTEGER,
            name TEXT
        )"#,
            Ok(Payload::Create),
        ),
    ];
    test_cases
        .into_iter()
        .for_each(|(sql, expected)| assert_eq!(expected, run(sql)));
}
