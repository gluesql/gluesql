CREATE TABLE Item (name TEXT DEFAULT LEFT('abc', 1))

-- expect: payload Create

INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')

-- expect: payload Insert
-- 3

CREATE TABLE SingleItem (id INTEGER)

-- expect: payload Create

INSERT INTO SingleItem VALUES (0)

-- expect: payload Insert
-- 1

CREATE TABLE NullName (name TEXT NULL)

-- expect: payload Create

INSERT INTO NullName VALUES (NULL)

-- expect: payload Insert
-- 1

CREATE TABLE NullNumber (number INTEGER NULL)

-- expect: payload Create

INSERT INTO NullNumber VALUES (NULL)

-- expect: payload Insert
-- 1

CREATE TABLE NullableName (name TEXT NULL)

-- expect: payload Create

INSERT INTO NullableName VALUES ('name')

-- expect: payload Insert
-- 1

SELECT LEFT(name, 3) AS test FROM Item

-- expect:
-- | test: Str |
-- | "Blo"     |
-- | "B"       |
-- | "Ste"     |

SELECT RIGHT(name, 10) AS test FROM Item

-- expect:
-- | test: Str    |
-- | "op mc blee" |
-- | "B"          |
-- | "d$ folken!" |

SELECT LEFT((name || 'bobbert'), 10) AS test FROM Item

-- expect:
-- | test: Str    |
-- | "Blop mc bl" |
-- | "Bbobbert"   |
-- | "Steven the" |

SELECT LEFT('blue', 10) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "blue"    |

SELECT LEFT('blunder', 3) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "blu"     |

SELECT LEFT(name, 3) AS test FROM NullName

-- expect:
-- | test |
-- | NULL |

SELECT LEFT('Words', number) AS test FROM NullNumber

-- expect:
-- | test |
-- | NULL |

SELECT LEFT(name, number) AS test FROM NullNumber INNER JOIN NullName ON 1 = 1

-- expect:
-- | test |
-- | NULL |

SELECT LEFT(name, 1) AS test FROM NullableName

-- expect:
-- | test: Str |
-- | "n"       |

SELECT RIGHT(name, 10, 10) AS test FROM SingleItem

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 2,
--   "found": 3,
--   "name": "RIGHT"
-- }

SELECT RIGHT(name) AS test FROM SingleItem

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 2,
--   "found": 1,
--   "name": "RIGHT"
-- }

SELECT RIGHT() AS test FROM SingleItem

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 2,
--   "found": 0,
--   "name": "RIGHT"
-- }

SELECT RIGHT(1, 1) AS test FROM SingleItem

-- expect: error Evaluate.FunctionRequiresStringValue
-- "RIGHT"

SELECT RIGHT('Words', 1.1) AS test FROM SingleItem

-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "RIGHT"

SELECT RIGHT('Words', -4) AS test FROM SingleItem

-- expect: error Evaluate.FunctionRequiresUSizeValue
-- "RIGHT"
