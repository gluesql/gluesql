CREATE TABLE Foo (geo1 Point, geo2 Point, bar Float)
-- @expect: payload Create

INSERT INTO Foo VALUES (POINT(0.3134, 3.156), POINT(1.415, 3.231), 3)
-- @expect: payload Insert
-- @json: 1

SELECT CALC_DISTANCE(geo1, geo2) AS georesult FROM Foo
-- @expect:
-- | georesult: F64 |
-- | -------------- |
-- | 1.104150152832 |

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
