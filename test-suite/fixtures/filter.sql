CREATE TABLE Boss (
    id INTEGER,
    name TEXT,
    strength FLOAT
);
-- @expect: ok

CREATE TABLE Hunter (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO Boss (id, name, strength) VALUES
    (1,    'Amelia', 10.10),
    (2,      'Doll', 20.20),
    (3, 'Gascoigne', 30.30),
    (4,   'Gehrman', 40.40),
    (5,     'Maria', 50.50);
-- @expect: ok

INSERT INTO Hunter (id, name) VALUES
    (1, 'Gascoigne'),
    (2,   'Gehrman'),
    (3,     'Maria');
-- @expect: ok

SELECT id, name FROM Boss WHERE id BETWEEN 2 AND 4
-- @expect: count 3

SELECT id, name FROM Boss WHERE name BETWEEN 'Doll' AND 'Gehrman'
-- @expect: count 3

SELECT name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'
-- @expect: count 2

SELECT strength, name FROM Boss WHERE name NOT BETWEEN 'Doll' AND 'Gehrman'
-- @expect: count 2

SELECT name
FROM Boss
WHERE EXISTS (
    SELECT * FROM Hunter WHERE Hunter.name = Boss.name
)
-- @expect: count 3

SELECT name
FROM Boss
WHERE NOT EXISTS (
    SELECT * FROM Hunter WHERE Hunter.name = Boss.name
)
-- @expect: count 2

SELECT name FROM Boss WHERE +1 = 1
-- @expect: count 5

SELECT id FROM Hunter WHERE -1 = -1
-- @expect: count 3

SELECT name FROM Boss WHERE -2.0 < -1.0
-- @expect: count 5

SELECT id FROM Hunter WHERE +2 > +1.0
-- @expect: count 3

SELECT name FROM Boss WHERE id <= +2
-- @expect: count 2

SELECT name FROM Boss WHERE +id <= 2
-- @expect: count 2

SELECT name FROM Boss WHERE 2 = 1.0 + 1
-- @expect: count 5

SELECT id FROM Hunter WHERE -1.0 - 1.0 < -1
-- @expect: count 3

SELECT name FROM Boss WHERE -2.0 * -3.0 = 6
-- @expect: count 5

SELECT id FROM Hunter WHERE +2 / 1.0 > +1.0
-- @expect: count 3

SELECT id FROM Hunter WHERE +'abcd' > 1.0
-- @expect: error Evaluate.UnsupportedUnaryPlus
-- @json: "abcd"

SELECT id FROM Hunter WHERE -'abcd' < 1.0
-- @expect: error Evaluate.UnsupportedUnaryMinus
-- @json: "abcd"

SELECT id FROM Hunter WHERE +name > 1.0
-- @expect: error Value.UnaryPlusOnNonNumeric

SELECT id FROM Hunter WHERE -name < 1.0
-- @expect: error Value.UnaryMinusOnNonNumeric
