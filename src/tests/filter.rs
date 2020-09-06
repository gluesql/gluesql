use crate::*;

pub fn filter(mut tester: impl tests::Tester) {
    let create_sqls = [
        "
        CREATE TABLE Boss (
            id INTEGER,
            name TEXT,
            strength FLOAT
        );",
        "
        CREATE TABLE Hunter (
            id INTEGER,
            name TEXT
        );",
    ];

    create_sqls.iter().for_each(|sql| tester.run_and_print(sql));

    let insert_sqls = [
        "INSERT INTO Boss (id, name, strength) VALUES (1, \"Amelia\", 10.10);",
        "INSERT INTO Boss (id, name, strength) VALUES (2, \"Doll\", 20.20);",
        "INSERT INTO Boss (id, name, strength) VALUES (3, \"Gascoigne\", 30.30);",
        "INSERT INTO Boss (id, name, strength) VALUES (4, \"Gehrman\", 40.40);",
        "INSERT INTO Boss (id, name, strength) VALUES (5, \"Maria\", 50.50);",
        "INSERT INTO Hunter (id, name) VALUES (1, \"Gascoigne\");",
        "INSERT INTO Hunter (id, name) VALUES (2, \"Gehrman\");",
        "INSERT INTO Hunter (id, name) VALUES (3, \"Maria\");",
    ];

    insert_sqls.iter().for_each(|sql| tester.run_and_print(sql));

    let select_sqls = [
        (3, "SELECT id, name FROM Boss WHERE id BETWEEN 2 AND 4"),
        (
            3,
            "SELECT id, name FROM Boss WHERE name BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            2,
            "SELECT name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            2,
            "SELECT strength, name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'",
        ),
        (
            3,
            "SELECT name 
             FROM Boss 
             WHERE EXISTS (
                SELECT * FROM Hunter WHERE Hunter.name = Boss.name
             )",
        ),
        (
            2,
            "SELECT name 
             FROM Boss 
             WHERE NOT EXISTS (
                SELECT * FROM Hunter WHERE Hunter.name = Boss.name
             )",
        ),
    ];

    select_sqls
        .iter()
        .for_each(|(num, sql)| tester.test_rows(sql, *num));
}
