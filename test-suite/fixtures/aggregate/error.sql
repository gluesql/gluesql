CREATE TABLE Item (
    id INTEGER,
    quantity INTEGER,
    age INTEGER NULL,
    total INTEGER
);

-- expect: ok

INSERT INTO Item (id, quantity, age, total) VALUES
    (1, 10,   11, 1),
    (2,  0,   90, 2),
    (3,  9, NULL, 3),
    (4,  3,    3, 1),
    (5, 25, NULL, 1);

-- expect: ok

SELECT SUM(num) FROM Item;

-- expect: error Evaluate.IdentifierNotFound
-- "num"

SELECT COUNT(Foo.*) FROM Item;

-- expect: error Translate.QualifiedWildcardInCountNotSupported
-- "Foo.*"

SELECT SUM(*) FROM Item;

-- expect: error Translate.WildcardFunctionArgNotAccepted
