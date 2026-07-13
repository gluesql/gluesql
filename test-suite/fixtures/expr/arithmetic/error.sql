CREATE TABLE Arith (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- expect: ok

DELETE FROM Arith
-- expect: ok

INSERT INTO Arith (id, num, name) VALUES
    (1, 6, 'A'),
    (2, 8, 'B'),
    (3, 4, 'C'),
    (4, 2, 'D'),
    (5, 3, 'E');
-- expect: ok

SELECT * FROM Arith WHERE name + id < 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Str": "A"
--   },
--   "operator": "Add",
--   "rhs": {
--     "I64": 1
--   }
-- }

SELECT * FROM Arith WHERE name - id < 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Str": "A"
--   },
--   "operator": "Subtract",
--   "rhs": {
--     "I64": 1
--   }
-- }

SELECT * FROM Arith WHERE name * id < 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Str": "A"
--   },
--   "operator": "Multiply",
--   "rhs": {
--     "I64": 1
--   }
-- }

SELECT * FROM Arith WHERE name / id < 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Str": "A"
--   },
--   "operator": "Divide",
--   "rhs": {
--     "I64": 1
--   }
-- }

SELECT * FROM Arith WHERE name % id < 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Str": "A"
--   },
--   "operator": "Modulo",
--   "rhs": {
--     "I64": 1
--   }
-- }

UPDATE Arith SET aaa = 1
-- expect: error Update.ColumnNotFound
-- "aaa"

SELECT * FROM Arith WHERE TRUE + 1 = 1
-- expect: error Value.NonNumericMathOperation
-- {
--   "lhs": {
--     "Bool": true
--   },
--   "operator": "Add",
--   "rhs": {
--     "I64": 1
--   }
-- }

SELECT * FROM Arith WHERE id = 2 / 0
-- expect: error Evaluate.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE id = 2 / 0.0
-- expect: error Evaluate.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0
-- expect: error Value.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE id = INTERVAL '2' HOUR / 0.0
-- expect: error Value.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE id = 2 % 0
-- expect: error Evaluate.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE id = 2 % 0.0
-- expect: error Evaluate.DivisorShouldNotBeZero

SELECT * FROM Arith WHERE TRUE AND 'hello'
-- expect: error Evaluate.BooleanTypeRequired
-- "hello"

SELECT * FROM Arith WHERE name AND id
-- expect: error Evaluate.BooleanTypeRequired
-- "A"
