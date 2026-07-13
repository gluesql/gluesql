CREATE FUNCTION add_none()

-- expect: error Translate.UnsupportedEmptyFunctionBody

CREATE FUNCTION add_none() RETURN null

-- expect: payload Create

CREATE FUNCTION add_zero(n INT) RETURN n

-- expect: payload Create

CREATE FUNCTION add_zero(n INT) RETURN n

-- expect: error Alter.FunctionAlreadyExists
-- "add_zero"

CREATE FUNCTION add_one (n INT, x INT DEFAULT 1) RETURN n + x

-- expect: payload Create

CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT) RETURN n + x + y

-- expect: error Alter.NonDefaultArgumentFollowsDefaultArgument

CREATE FUNCTION add_two (n INT, x INT DEFAULT 1, y INT DEFAULT 1) RETURN n + x + y

-- expect: payload Create

SELECT add_none() AS r

-- expect:
-- | r    |
-- | NULL |

SELECT add_one(1) AS r

-- expect:
-- | r: I64 |
-- | 2      |

SELECT add_one(1, 8) AS r

-- expect:
-- | r: I64 |
-- | 9      |

SELECT add_one(1, 2, 4)

-- expect: error Evaluate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 3,
--   "name": "add_one"
-- }

SELECT add_one()

-- expect: error Evaluate.FunctionArgsLengthNotWithinRange
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 0,
--   "name": "add_one"
-- }

SELECT add_two(1, null, 2) as r

-- expect:
-- | r    |
-- | NULL |

SELECT add_two(1) as r

-- expect:
-- | r: I64 |
-- | 3      |

DROP FUNCTION add_none

-- expect: payload DropFunction

SHOW FUNCTIONS

-- expect: payload ShowVariable.Functions
-- [
--   "add_one(n: INT, x: INT)",
--   "add_two(n: INT, x: INT, y: INT)",
--   "add_zero(n: INT)"
-- ]

DROP FUNCTION add_none

-- expect: error Alter.FunctionNotFound
-- "add_none"

DROP FUNCTION IF EXISTS add_zero, add_one, add_two

-- expect: payload DropFunction

CREATE FUNCTION test(INT) RETURN 1

-- expect: error Translate.UnNamedFunctionArgNotSupported

CREATE FUNCTION test(a INT DEFAULT test()) RETURN 1

-- expect: error Evaluate.UnsupportedCustomFunction

CREATE FUNCTION test(a INT, a BOOLEAN) RETURN 1

-- expect: error Alter.DuplicateArgName
-- "a"
