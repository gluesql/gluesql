CREATE TABLE PointGroup (point_field POINT)
-- @expect: payload Create

INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156))
-- @expect: payload Insert
-- @json: 1

SELECT GET_X(point_field) AS point_field FROM PointGroup;
-- @expect:
-- | point_field: F64 |
-- | 0.3134           |

SELECT GET_X(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx
-- @expect:
-- | ptx: F64 |
-- | 0.1      |

SELECT GET_X(POINT(0.1, -0.2)) AS ptx
-- @expect:
-- | ptx: F64 |
-- | 0.1      |

SELECT GET_X('cheese') AS ptx
-- @expect: error Evaluate.FunctionRequiresPointValue
-- @json: "GET_X"
