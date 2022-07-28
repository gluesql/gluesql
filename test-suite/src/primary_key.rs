use crate::*;

test_case!(primary_key, async move {
    run!(
        "
        CREATE TABLE Allegro (
            id INTEGER PRIMARY KEY,
            name TEXT,
        );
    "
    );
    run!(
        "INSERT INTO Allegro VALUES (1, 'hello'), (2, 'world');"
    )
    // update fail, insert duplicate fail
});
