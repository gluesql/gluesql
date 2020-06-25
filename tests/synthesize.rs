mod helper;

use helper::{Helper, SledHelper};

#[test]
fn synthesize() {
    println!("\n\n");

    let helper = SledHelper::new("data.db");

    let create_sql = "
        CREATE TABLE TableA (
            id INTEGER,
            test INTEGER,
            target_id INTEGER,
        );
    ";

    helper.run_and_print(create_sql);

    let delete_sql = "DELETE FROM TableA";
    helper.run_and_print(delete_sql);

    let insert_sqls = [
        "INSERT INTO TableA (id, test, target_id) VALUES (1, 100, 2);",
        "INSERT INTO TableA (id, test, target_id) VALUES (2, 100, 1);",
        "INSERT INTO TableA (id, test, target_id) VALUES (3, 300, 5);",
        "INSERT INTO TableA (target_id, id, test) VALUES (5, 3, 400);",
        "INSERT INTO TableA (test, id, target_id) VALUES (500, 3, 4);",
        "INSERT INTO TableA VALUES (4, 500, 3);",
    ];

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    let test_cases = [
        (6, "SELECT * FROM TableA;"),
        (3, "SELECT * FROM TableA WHERE id = 3;"),
        (
            3,
            "SELECT * FROM TableA WHERE id = (SELECT id FROM TableA WHERE id = 3 LIMIT 1)",
        ),
        (3, "SELECT * FROM TableA WHERE id IN (1, 2, 4)"),
        (3, "SELECT * FROM TableA WHERE test IN (500, 300)"),
        (
            2,
            "SELECT * FROM TableA WHERE id IN (SELECT target_id FROM TableA LIMIT 3)",
        ),
        (1, "SELECT * FROM TableA WHERE id = 3 AND test = 500;"),
        (5, "SELECT * FROM TableA WHERE id = 3 OR test = 100;"),
        (1, "SELECT * FROM TableA WHERE id != 3 AND test != 100;"),
        (2, "SELECT * FROM TableA WHERE id = 3 LIMIT 2;"),
        (4, "SELECT * FROM TableA LIMIT 10 OFFSET 2;"),
        /*
        (
            1,
            "SELECT * FROM TableA WHERE (id = 3 OR test = 100) AND test = 300;",
        ),
        */
        (4, "SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA b WHERE b.target_id = a.id LIMIT 1);"),
        (4, "SELECT * FROM TableA a WHERE target_id = (SELECT id FROM TableA WHERE target_id = a.id LIMIT 1);"),
        // (3, "SELECT * FROM TableA WHERE NOT (id = 3);"),
        (2, "UPDATE TableA SET test = 200 WHERE test = 100;"),
        (0, "SELECT * FROM TableA WHERE test = 100;"),
        // (2, "SELECT * FROM TableA WHERE (test = 200);"),
        (3, "DELETE FROM TableA WHERE id != 3;"),
        (3, "SELECT * FROM TableA;"),
        (3, "DELETE FROM TableA;"),
    ];

    for (num, sql) in test_cases.iter() {
        helper.test_rows(sql, *num);
    }

    for insert_sql in insert_sqls.iter() {
        helper.run(insert_sql).unwrap();
    }

    let test_select = |sql, num| {
        helper.test_columns(sql, num);
    };

    let test_cases = [
        (2, "SELECT id, test FROM TableA;"),
        (1, "SELECT id FROM TableA;"),
        (3, "SELECT * FROM TableA;"),
    ];

    for (num, sql) in test_cases.iter() {
        test_select(sql, *num);
    }

    println!("\n\n");
}
