CREATE TABLE Item (name TEXT DEFAULT LPAD('a', 5) || LPAD('b', 3))

-- expect: payload Create

INSERT INTO Item VALUES ('hello')

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

SELECT LPAD(name, 10), RPAD(name, 10) FROM Item

-- expect:
-- | LPAD(name, 10): Str | RPAD(name, 10): Str |
-- | "     hello"        | "hello     "        |

SELECT LPAD(name, 10, 'ab'), RPAD(name, 10, 'ab') FROM Item

-- expect:
-- | LPAD(name, 10, 'ab'): Str | RPAD(name, 10, 'ab'): Str |
-- | "ababahello"              | "helloababa"              |

SELECT LPAD(name, 3), RPAD(name, 3) FROM Item

-- expect:
-- | LPAD(name, 3): Str | RPAD(name, 3): Str |
-- | "hel"              | "hel"              |

SELECT LPAD(name, 3, 'ab'), RPAD(name, 3, 'ab') FROM Item

-- expect:
-- | LPAD(name, 3, 'ab'): Str | RPAD(name, 3, 'ab'): Str |
-- | "hel"                    | "hel"                    |

SELECT LPAD(name, 10, 'ab') AS lpad FROM NullName

-- expect:
-- | lpad |
-- | NULL |

SELECT RPAD(name, 10, 'ab') AS rpad FROM NullName

-- expect:
-- | rpad |
-- | NULL |

SELECT LPAD('hello', number, 'ab') AS lpad FROM NullNumber

-- expect:
-- | lpad |
-- | NULL |

SELECT RPAD('hello', number, 'ab') AS rpad FROM NullNumber

-- expect:
-- | rpad |
-- | NULL |

SELECT LPAD('hello', 10, name) AS lpad FROM NullName

-- expect:
-- | lpad |
-- | NULL |

SELECT RPAD('hello', 10, name) AS rpad FROM NullName

-- expect:
-- | rpad |
-- | NULL |

SELECT LPAD(name) FROM Item

-- expect: error Translate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 3,
--   "expected_minimum": 2,
--   "found": 1,
--   "name": "LPAD"
-- }

SELECT RPAD(name) FROM Item

-- expect: error Translate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 3,
--   "expected_minimum": 2,
--   "found": 1,
--   "name": "RPAD"
-- }

SELECT LPAD(name, 10, 'ab', 'cd') FROM Item

-- expect: error Translate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 3,
--   "expected_minimum": 2,
--   "found": 4,
--   "name": "LPAD"
-- }

SELECT RPAD(name, 10, 'ab', 'cd') FROM Item

-- expect: error Translate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 3,
--   "expected_minimum": 2,
--   "found": 4,
--   "name": "RPAD"
-- }

SELECT LPAD(1, 10, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresStringValue
-- "LPAD"

SELECT RPAD(1, 10, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresStringValue
-- "RPAD"

SELECT LPAD(name, -10, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresUSizeValue
-- "LPAD"

SELECT RPAD(name, -10, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresUSizeValue
-- "RPAD"

SELECT LPAD(name, 10.1, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "LPAD"

SELECT RPAD(name, 10.1, 'ab') FROM Item

-- expect: error Evaluate.FunctionRequiresIntegerValue
-- "RPAD"
