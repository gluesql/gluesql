mod helper;

use gluesql::{Payload, SledStorage};
use helper::{compare, print, run};

#[test]
fn synthesize() {
    println!("\n\n");

    let storage = SledStorage::new(String::from("data.db"));
    let run_sql = |sql| run(&storage, sql);

    let create_sql = "
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
            target_id INTEGER,
        );
    ";

    print(run_sql(create_sql));

    let delete_sql = "DELETE FROM TableA";
    print(run_sql(delete_sql));

    let insert_sqls: [&str; 6] = [
        "INSERT INTO TableA (id, test, target_id) VALUES (1, 100, 2);",
        "INSERT INTO TableA (id, test, target_id) VALUES (2, 100, 1);",
        "INSERT INTO TableA (id, test, target_id) VALUES (3, 300, 5);",
        "INSERT INTO TableA (id, test, target_id) VALUES (3, 400, 5);",
        "INSERT INTO TableA (id, test, target_id) VALUES (3, 500, 4);",
        "INSERT INTO TableA (id, test, target_id) VALUES (4, 500, 3);",
    ];

    for insert_sql in insert_sqls.iter() {
        run_sql(insert_sql).unwrap();
    }

    let test_cases = vec![
        (6, "SELECT * FROM TableA;"),
        (3, "SELECT * FROM TableA WHERE id = 3;"),
        (
            3,
            "SELECT * FROM TableA WHERE id = (SELECT id FROM TableA WHERE id = 3 LIMIT 1)",
        ),
        (1, "SELECT * FROM TableA WHERE id = 3 AND test = 500;"),
        (5, "SELECT * FROM TableA WHERE id = 3 OR test = 100;"),
        (1, "SELECT * FROM TableA WHERE id != 3 AND test != 100;"),
        (2, "SELECT * FROM TableA WHERE id = 3 LIMIT 2;"),
        (4, "SELECT * FROM TableA LIMIT 10 OFFSET 2;"),
        (
            1,
            "SELECT * FROM TableA WHERE (id = 3 OR test = 100) AND test = 300;",
        ),
        (4, "SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA b WHERE b.target_id = a.id LIMIT 1);"),
        (4, "SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA WHERE target_id = a.id LIMIT 1);"),
        (3, "SELECT * FROM TableA WHERE NOT (id = 3);"),
        (2, "UPDATE TableA SET test = 200 WHERE test = 100;"),
        (0, "SELECT * FROM TableA WHERE test = 100;"),
        (2, "SELECT * FROM TableA WHERE (test = 200);"),
        (3, "DELETE FROM TableA WHERE id != 3;"),
        (3, "SELECT * FROM TableA;"),
        (3, "DELETE FROM TableA;"),
    ];

    for (num, sql) in test_cases {
        compare(run_sql(sql), num);
    }

    for insert_sql in insert_sqls.iter() {
        run_sql(insert_sql).unwrap();
    }

    let test_select = |sql, num| {
        match run_sql(sql).unwrap() {
            Payload::Select(rows) => assert_eq!(rows.into_iter().nth(0).unwrap().items.len(), num),
            _ => assert!(false),
        };
    };

    let select_sql = "SELECT id, test FROM TableA;";
    test_select(select_sql, 2);

    let select_sql = "SELECT id FROM TableA;";
    test_select(select_sql, 1);

    let select_sql = "SELECT * FROM TableA;";
    test_select(select_sql, 3);

    println!("\n\n");
}
