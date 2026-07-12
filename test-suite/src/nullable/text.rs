use super::*;

test_case!(text, {
    let g = get_tester!();

    g.run(
        "
        CREATE TABLE Foo (
            id INTEGER,
            name TEXT NULL
        );
    ",
    );

    g.run("INSERT INTO Foo (id, name) VALUES (1, 'Hello'), (2, Null);");
});
