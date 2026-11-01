CREATE FUNCTION inc(x INT) RETURN x + 1;
-- @expect: payload Create

-- @name: VALUES expressions can call custom functions
VALUES (inc(1));
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 2            |

-- @name: A custom function can determine the size of SERIES
SELECT * FROM SERIES(inc(1));
-- @expect:
-- | N: I64 |
-- | ------ |
-- | 1      |
-- | 2      |

-- @name: VALUES ordering and bounds can call custom functions
VALUES (inc(1)), (inc(2)), (inc(3))
ORDER BY inc(column1) DESC LIMIT inc(0) OFFSET inc(0);
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 3            |

CREATE TABLE Item (x INT);
-- @expect: payload Create

-- @name: INSERT VALUES can call custom functions
INSERT INTO Item VALUES (inc(0)), (inc(1)), (inc(2));
-- @expect: payload Insert
-- @json: 3

-- @name: INSERT SELECT can call custom functions
INSERT INTO Item SELECT inc(N) FROM SERIES(2);
-- @expect: payload Insert
-- @json: 2

-- @name: UPDATE assignments and filters can call custom functions
UPDATE Item SET x = inc(x) WHERE x = inc(1);
-- @expect: payload Update
-- @json: 2

-- @name: DELETE filters can call custom functions
DELETE FROM Item WHERE inc(x) = 4;
-- @expect: payload Delete
-- @json: 4

-- @name: CREATE TABLE AS SELECT can call custom functions
CREATE TABLE Result AS SELECT inc(x) AS y FROM Item;
-- @expect: payload Create

-- @name: The created table contains the function result
SELECT y FROM Result;
-- @expect:
-- | y: I64 |
-- | ------ |
-- | 2      |

CREATE TABLE OrderedValues (x INT, fallback INT DEFAULT 7);
-- @expect: payload Create

-- @name: INSERT VALUES ordering and bounds can call custom functions
INSERT INTO OrderedValues (x) VALUES (inc(0)), (inc(1)), (inc(2))
ORDER BY inc(column1) DESC LIMIT inc(0) OFFSET inc(0);
-- @expect: payload Insert
-- @json: 1

-- @name: INSERT still applies defaults to omitted columns
SELECT x, fallback FROM OrderedValues;
-- @expect:
-- | x: I64 | fallback: I64 |
-- | ------ | ------------- |
-- | 2      | 7             |

CREATE FUNCTION document() RETURN '{"x": 2}';
-- @expect: payload Create

CREATE TABLE FunctionDocs;
-- @expect: payload Create

-- @name: A function result can supply a schemaless INSERT value
INSERT INTO FunctionDocs VALUES (document());
-- @expect: payload Insert
-- @json: 1

-- @name: The schemaless row contains the returned document
SELECT x FROM FunctionDocs;
-- @expect:
-- | x: I64 |
-- | ------ |
-- | 2      |

-- @name: CREATE TABLE AS VALUES can call custom functions
CREATE TABLE FunctionValues AS VALUES (inc(0));
-- @expect: payload Create

-- @name: The created VALUES column contains the function result
SELECT column1 FROM FunctionValues;
-- @expect:
-- | column1: I64 |
-- | ------------ |
-- | 1            |

-- @name: Calling a custom function in a column default is rejected
CREATE TABLE Defaults (x INT DEFAULT inc(1));
-- @expect: error Evaluate.UnplannedCustomFunction
-- @json: "INC"
