use super::*;

test_case!(implicit_insert, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        );
    ",
    );

    g.run("INSERT INTO Foo (id) VALUES (1)");
    g.test(
        "SELECT id, name FROM Foo",
        Ok(select_with_null!(
            id   | name;
            I64(1)  Null
        )),
    );
});
