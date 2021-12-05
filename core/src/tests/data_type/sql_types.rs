use crate::*;

test_case!(sql_types, async move {
    run!(
        "
        CREATE TABLE Item (
            id INTEGER,
            content TEXT,
            verified BOOLEAN,
            ratio FLOAT
        );
    "
    );
    run!(
        "
        INSERT INTO Item
            (id,   content, verified, ratio)
        VALUES
            ( 1, \"Hello\",     True,   0.1),
            ( 1, \"World\",    False,   0.9),
            ( 1,    'test',    False,   0.0);
    "
    );

    let test_sqls = [
        (3, "SELECT * FROM Item;"),
        (1, "SELECT * FROM Item WHERE verified = True;"),
        (1, "SELECT * FROM Item WHERE ratio > 0.5;"),
        (1, "SELECT * FROM Item WHERE ratio = 0.1;"),
        (
            1,
            "UPDATE Item SET content=\"Foo\" WHERE content=\"World\";",
        ),
        (0, "SELECT * FROM Item WHERE content=\"World\";"),
        (1, "SELECT * FROM Item WHERE content=\"Foo\";"),
        (1, "SELECT * FROM Item WHERE content='Foo';"),
        (1, "UPDATE Item SET id = 11 WHERE content=\"Foo\";"),
        (1, "UPDATE Item SET id = 14 WHERE content='Foo';"),
        (3, "SELECT * FROM Item;"),
    ];

    for (num, sql) in test_sqls.iter() {
        count!(*num, sql);
    }

    run!("DELETE FROM Item");
});
