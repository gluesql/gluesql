CREATE TABLE TypeMatch (uuid_value UUID, float_value FLOAT, int_value INT, bool_value BOOLEAN)
-- expect: ok

INSERT INTO TypeMatch values(GENERATE_UUID(), 1.0, 1, true)
-- expect: ok

SELECT * FROM TypeMatch
-- expect: types
-- | Uuid | Float | Int | Boolean |
