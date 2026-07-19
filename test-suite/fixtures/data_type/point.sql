CREATE TABLE POINT (point_field POINT)
-- @expect: payload Create

INSERT INTO POINT VALUES (POINT(0.3134, 0.156))
-- @expect: payload Insert
-- @json: 1

SELECT point_field AS point_field FROM POINT;
-- @expect:
-- | point_field: Point    |
-- | --------------------- |
-- | "POINT(0.3134 0.156)" |

UPDATE POINT SET point_field=POINT(2.0, 1.0) WHERE point_field=POINT(0.3134, 0.156)
-- @expect: payload Update
-- @json: 1

SELECT point_field AS point_field FROM POINT
-- @expect:
-- | point_field: Point |
-- | ------------------ |
-- | "POINT(2 1)"       |

DELETE FROM POINT WHERE point_field=POINT(2.0, 1.0)
-- @expect: payload Delete
-- @json: 1

INSERT INTO POINT VALUES (0)
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Point",
--   "literal": "0"
-- }

INSERT INTO POINT VALUES (POINT(0.3134))
-- @expect: error Translate.FunctionArgsLengthNotMatching
-- @json:
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "POINT"
-- }

SELECT CAST('POINT(-71.064544 42.28787)' AS POINT) AS pt
-- @expect:
-- | pt: Point                    |
-- | ---------------------------- |
-- | "POINT(-71.064544 42.28787)" |

SELECT CAST('POINT(-71.06454t4 42.28787)' AS POINT) AS pt
-- @expect: error Evaluate.TextParseFailed
-- @json:
-- {
--   "data_type": "Point",
--   "literal": "POINT(-71.06454t4 42.28787)"
-- }
