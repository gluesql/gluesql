CREATE TABLE Foo (geo1 Point, geo2 Point, bar Float)
-- @expect: payload Create

INSERT INTO Foo VALUES (POINT(0.0, 0.0), POINT(3.0, 4.0), 3)
-- @expect: payload Insert
-- @json: 1

SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo
-- @expect:
-- | georesult: F64 |
-- | -------------- |
-- | 5.0            |

SELECT CALC_DISTANCE(geo1) AS georesult FROM Foo
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "CALC_DISTANCE"
-- }

SELECT CALC_DISTANCE(geo1, bar) AS georesult FROM Foo
-- @expect: error Evaluate.FunctionRequiresPointValue
-- @json: "CALC_DISTANCE"

SELECT CALC_DISTANCE(geo1, NULL) AS georesult FROM Foo
-- @expect:
-- | georesult |
-- | --------- |
-- | NULL      |
