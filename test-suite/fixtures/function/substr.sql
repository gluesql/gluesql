CREATE TABLE Item (name TEXT DEFAULT SUBSTR('abc', 0, 2))

-- expect: payload Create

INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')

-- expect: payload Insert
-- 3

CREATE TABLE SingleItem (food TEXT)

-- expect: payload Create

INSERT INTO SingleItem VALUES (SUBSTR('LobSter',1))

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

SELECT SUBSTR(SUBSTR(name, 1), 1) AS test FROM Item

-- expect:
-- | test: Str                         |
-- | "Blop mc blee"                    |
-- | "B"                               |
-- | "Steven the &long named$ folken!" |

SELECT * FROM Item WHERE name = SUBSTR('ABC', 2, 1)

-- expect:
-- | name: Str |
-- | "B"       |

SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = 'B'

-- expect:
-- | name: Str      |
-- | "Blop mc blee" |
-- | "B"            |

SELECT * FROM Item WHERE 'B' = SUBSTR(name, 1, 1)

-- expect:
-- | name: Str      |
-- | "Blop mc blee" |
-- | "B"            |

SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = UPPER('b')

-- expect:
-- | name: Str      |
-- | "Blop mc blee" |
-- | "B"            |

SELECT * FROM Item WHERE SUBSTR(name, 1, 4) = SUBSTR('Blop', 1)

-- expect:
-- | name: Str      |
-- | "Blop mc blee" |

SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > SUBSTR('Blop', 1)

-- expect:
-- | name: Str                         |
-- | "Steven the &long named$ folken!" |

SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > 'B'

-- expect:
-- | name: Str                         |
-- | "Blop mc blee"                    |
-- | "Steven the &long named$ folken!" |

SELECT * FROM Item WHERE 'B' < SUBSTR(name, 1, 4)

-- expect:
-- | name: Str                         |
-- | "Blop mc blee"                    |
-- | "Steven the &long named$ folken!" |

SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > UPPER('b')

-- expect:
-- | name: Str                         |
-- | "Blop mc blee"                    |
-- | "Steven the &long named$ folken!" |

SELECT * FROM Item WHERE UPPER('b') < SUBSTR(name, 1, 4)

-- expect:
-- | name: Str                         |
-- | "Blop mc blee"                    |
-- | "Steven the &long named$ folken!" |

SELECT SUBSTR(name, 2) AS test FROM Item

-- expect:
-- | test: Str                        |
-- | "lop mc blee"                    |
-- | ""                               |
-- | "teven the &long named$ folken!" |

SELECT SUBSTR(name, 999) AS test FROM Item

-- expect:
-- | test: Str |
-- | ""        |
-- | ""        |
-- | ""        |

SELECT SUBSTR('ABC', -3, 0) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | ""        |

SELECT SUBSTR('ABC', 0, 3) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "AB"      |

SELECT SUBSTR('ABC', 1, 3) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "ABC"     |

SELECT SUBSTR('ABC', 1, 999) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "ABC"     |

SELECT SUBSTR('ABC', -1000, 1003) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "AB"      |

SELECT SUBSTR('ABC', -1, 3) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "A"       |

SELECT SUBSTR('ABC', -1, 4) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "AB"      |

SELECT SUBSTR(SUBSTR('ABC', 2, 3), 1, 1) AS test FROM SingleItem

-- expect:
-- | test: Str |
-- | "B"       |

SELECT SUBSTR('ABC', -1, NULL) AS test FROM SingleItem

-- expect:
-- | test |
-- | NULL |

SELECT SUBSTR(name, 3) AS test FROM NullName

-- expect:
-- | test |
-- | NULL |

SELECT SUBSTR('Words', number) AS test FROM NullNumber

-- expect:
-- | test |
-- | NULL |

SELECT * FROM SingleItem WHERE TRUE AND SUBSTR('wine',2,3)

-- expect: error Evaluate.BooleanTypeRequired
-- "ine"

SELECT SUBSTR(1, 1) AS test FROM SingleItem

-- expect: error Evaluate.FunctionRequiresStringValue
-- "SUBSTR"

SELECT SUBSTR('Words', 1.1) AS test FROM SingleItem

-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "SUBSTR"

SELECT SUBSTR('Words', 1, -4) AS test FROM SingleItem

-- expect: error Evaluate.NegativeSubstrLenNotAllowed

SELECT SUBSTR('123', 2, 3) - '3' AS test FROM SingleItem

-- expect: error Evaluate.UnsupportedBinaryOperation
-- {
--   "left": "23",
--   "op": "Minus",
--   "right": "3"
-- }

SELECT +SUBSTR('123', 2, 3) AS test FROM SingleItem

-- expect: error Evaluate.UnsupportedUnaryPlus
-- "23"

SELECT -SUBSTR('123', 2, 3) AS test FROM SingleItem

-- expect: error Evaluate.UnsupportedUnaryMinus
-- "23"

SELECT SUBSTR('123', 2, 3)! AS test FROM SingleItem

-- expect: error Evaluate.UnaryFactorialRequiresNumericLiteral
-- "23"

SELECT ~SUBSTR('123', 2, 3) AS test FROM SingleItem

-- expect: error Evaluate.UnaryBitwiseNotRequiresIntegerLiteral
-- "23"
