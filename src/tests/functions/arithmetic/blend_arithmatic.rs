use crate::*;

test_case!(async move {
    run!(
        "
        CREATE TABLE Arith (
            id INTEGER,
            num INTEGER,
        );
    "
    );
    run!("DELETE FROM Arith");
    run!(
        "
        INSERT INTO Arith (id, num) VALUES
            (1, 6),
            (2, 8),
            (3, 4),
            (4, 2),
            (5, 3);
    "
    );

    use Value::I64;

    let sql = "SELECT 1 * 2 + 1 - 3 / 1 FROM Arith LIMIT 1;";
    let found = run!(sql);
    let expected = select!("1 * 2 + 1 - 3 / 1"; I64; 0);
    assert_eq!(expected, found);

    let found = run!("SELECT id, id + 1, id + num, 1 + 1 FROM Arith");
    let expected = select!(
        id  | "id + 1" | "id + num" | "1 + 1"
        I64 | I64      | I64        | I64;
        1     2          7            2;
        2     3          10           2;
        3     4          7            2;
        4     5          6            2;
        5     6          8            2
    );
    assert_eq!(expected, found);

    let sql = "
      SELECT a.id + b.id
      FROM Arith a
      JOIN Arith b ON a.id = b.id + 1
    ";
    let found = run!(sql);
    let expected = select!("a.id + b.id"; I64; 3; 5; 7; 9);
    assert_eq!(expected, found);
});
