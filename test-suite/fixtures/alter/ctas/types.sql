-- @name: The first typed VALUES expression determines the column type
CREATE TABLE FloatFirst AS VALUES (CAST(1 AS FLOAT)), (2);
-- @expect: ok

SELECT * FROM FloatFirst;
-- @expect:
-- | column1: F64 |
-- | ------------ |
-- | 1.0          |
-- | 2.0          |

-- @name: Later typed values do not widen the first type
CREATE TABLE IntFirst AS VALUES (2), (CAST(1 AS FLOAT));
-- @expect: error Planner.CtasColumnTypeMismatch
-- @json: {"column":"column1","expected":"Int","found":"Float"}

-- @name: Untyped NULL does not supply a type
CREATE TABLE NullFirst AS VALUES (NULL), (1);
-- @expect: ok

SELECT * FROM NullFirst;
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | NULL         |
-- | 1            |

CREATE TABLE OnlyNull AS VALUES (NULL), (NULL);
-- @expect: error Planner.CtasColumnTypeUnknown
-- @json: "column1"

-- @name: CAST supplies a type even when every value is NULL
CREATE TABLE TypedNull AS VALUES (CAST(NULL AS FLOAT)), (NULL);
-- @expect: ok

SHOW COLUMNS FROM TypedNull;
-- @expect: payload ShowColumns
-- @json: [["column1","Float"]]

CREATE TABLE Amount (value FLOAT);
-- @expect: ok

INSERT INTO Amount VALUES (NULL);
-- @expect: ok

-- @name: A nullable scalar subquery keeps its schema type in nested VALUES
CREATE TABLE NestedValues AS
SELECT value FROM (VALUES ((SELECT value FROM Amount)), (2)) v(value);
-- @expect: ok

SELECT value FROM NestedValues;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | NULL       |
-- | 2.0        |

CREATE TABLE Branches (flag BOOLEAN, amount FLOAT);
-- @expect: ok

INSERT INTO Branches VALUES (TRUE, NULL), (FALSE, 2.5);
-- @expect: ok

-- @name: CASE literals use the typed alternative regardless of branch order
CREATE TABLE Chosen AS
SELECT CASE WHEN flag THEN 0 ELSE amount END AS value FROM Branches;
-- @expect: ok

SELECT * FROM Chosen;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | 0.0        |
-- | 2.5        |

CREATE TABLE Reversed AS
SELECT CASE WHEN flag THEN amount ELSE 0 END AS value FROM Branches;
-- @expect: ok

SELECT * FROM Reversed;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | NULL       |
-- | 0.0        |

-- @name: COALESCE and IFNULL prepare their literal alternatives too
CREATE TABLE Fallbacks AS
SELECT COALESCE(amount, 0) AS first, IFNULL(amount, 0) AS second FROM Branches;
-- @expect: ok

SELECT * FROM Fallbacks;
-- @expect:
-- | first: F64 | second: F64 |
-- | ---------- | ----------- |
-- | 0.0        | 0.0         |
-- | 2.5        | 2.5         |

-- @name: Aggregate execution slots use the prepared CASE values
CREATE TABLE Totals AS
SELECT SUM(CASE WHEN flag THEN 0 ELSE amount END) AS total FROM Branches;
-- @expect: ok

SELECT * FROM Totals;
-- @expect:
-- | total: F64 |
-- | ---------- |
-- | 2.5        |

-- @name: Aggregate copies in HAVING and ORDER BY retain their prepared slots
CREATE TABLE OrderedTotals AS
SELECT SUM(CASE WHEN flag THEN 0 ELSE amount END) AS total FROM Branches
HAVING SUM(CASE WHEN flag THEN 0 ELSE amount END) > 0
ORDER BY SUM(CASE WHEN flag THEN 0 ELSE amount END);
-- @expect: ok

SELECT * FROM OrderedTotals;
-- @expect:
-- | total: F64 |
-- | ---------- |
-- | 2.5        |

-- @name: ORDER BY can refer to an output alias inside an expression
CREATE TABLE OrderedAliases AS
SELECT amount AS value FROM Branches ORDER BY COALESCE(value, CAST(0 AS FLOAT));
-- @expect: ok

SELECT * FROM OrderedAliases;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | NULL       |
-- | 2.5        |

-- @name: A derived JOIN source uses the types of preceding source columns
CREATE TABLE Correlated AS
SELECT d.value FROM Branches b
JOIN (SELECT COALESCE(b.amount, 0) AS value) d;
-- @expect: ok

SELECT * FROM Correlated;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | 0.0        |
-- | 2.5        |

CREATE TABLE Conflicting AS
SELECT CASE WHEN flag THEN CAST(0 AS INT) ELSE amount END AS value FROM Branches;
-- @expect: error Planner.CtasColumnTypeMismatch

-- @name: An explicit outer CAST resolves different typed alternatives
CREATE TABLE CastBranches AS
SELECT CAST(CASE WHEN flag THEN CAST(0 AS INT) ELSE amount END AS FLOAT) AS value
FROM Branches;
-- @expect: ok

SELECT * FROM CastBranches;
-- @expect:
-- | value: F64 |
-- | ---------- |
-- | 0.0        |
-- | 2.5        |

-- @name: Literal arithmetic is converted as a whole
CREATE TABLE Fractions AS VALUES (1 / 2);
-- @expect: ok

SELECT * FROM Fractions;
-- @expect:
-- | column1: F64 |
-- | ------------ |
-- | 0.5          |

CREATE TABLE Docs;
-- @expect: ok

INSERT INTO Docs VALUES ('{"value": 3}');
-- @expect: ok

CREATE TABLE UnknownField AS SELECT value FROM Docs;
-- @expect: error Planner.CtasColumnTypeUnknown
-- @json: "value"

-- @name: An explicit definition supplies the type of a dynamic field
CREATE TABLE KnownField (value INT) AS SELECT value FROM Docs;
-- @expect: ok

SELECT * FROM KnownField;
-- @expect:
-- | value: I64 |
-- | ---------- |
-- | 3          |

CREATE TABLE MetadataCopy AS SELECT * FROM GLUE_OBJECTS;
-- @expect: error Planner.CtasWildcardColumnsUnknown

-- @name: Fixed metadata fields have known output types without scanning metadata
CREATE TABLE ObjectNames AS SELECT OBJECT_NAME FROM GLUE_OBJECTS WHERE FALSE;
-- @expect: ok

SHOW COLUMNS FROM ObjectNames;
-- @expect: payload ShowColumns
-- @json: [["OBJECT_NAME","Text"]]
