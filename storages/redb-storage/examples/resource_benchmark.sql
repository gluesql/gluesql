CREATE TABLE Items (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO Items VALUES
    (1, 'apple'),
    (2, 'banana'),
    (3, 'cherry');

SELECT * FROM Items WHERE id = 1;
SELECT * FROM Items;
