use crate::*;

test_case!(numeric, async move {
    run!("CREATE TABLE Item (small_int INT(8));");
    run!("INSERT INTO Item (small_int) VALUES (127), (-128);");

    // TODO: write test codes
    // let test_case = vec![(
    //     "SELECT COUNT(*) FROM Item WHERE small_int > 0",
    //     select!("COUNT(*)"; I64; 1),
    // )];

    // for (sql, expected) in test_case {
    //     test!(Ok(expected), sql);
    // }

    run!("DELETE FROM Item");
});
