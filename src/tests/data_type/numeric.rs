use crate::*;

test_case!(numeric, async move {
    run!(
        "
        CREATE TABLE Item (
		small_int INT(8)
        );
    "
    );
    run!(
        "
        INSERT INTO Item
            (small_int)
        VALUES
            (127);
            (-128);
	    "
    );

    let test_sqls = [
        (2, "SELECT * FROM Item;"),
        (1, "SELECT * FROM Item WHERE small_int > 0;"),
        (1, "SELECT * FROM Item WHERE small_int < 0;"),
    ];

    for (num, sql) in test_sqls {
        count!(num, sql);
    }

    run!("DELETE FROM Item");
});
