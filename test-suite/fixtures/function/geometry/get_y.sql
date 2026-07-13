CREATE TABLE PointGroup (point_field POINT)
-- expect: payload Create

INSERT INTO PointGroup VALUES (POINT(0.3134, 0.156))
-- expect: payload Insert
-- 1

SELECT GET_Y(point_field) AS point_field FROM PointGroup;
-- expect:
-- | point_field: F64 |
-- | 0.156            |

SELECT GET_Y(CAST('POINT(0.1 -0.2)' AS POINT)) AS ptx
-- expect:
-- | ptx: F64 |
-- | -0.2     |

SELECT GET_Y(POINT(0.1, -0.2)) AS ptx
-- expect:
-- | ptx: F64 |
-- | -0.2     |

SELECT GET_Y('cheese') AS ptx
-- expect: error Evaluate.FunctionRequiresPointValue
-- "GET_Y"
