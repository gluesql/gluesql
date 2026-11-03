CREATE FUNCTION add_one(n INT, x INT DEFAULT 1) RETURN n + x;
-- @expect: payload Create

-- @name: An omitted argument uses its default value
SELECT add_one(1) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

-- @name: An explicit argument overrides the default value
SELECT add_one(1, 8) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 9      |

CREATE FUNCTION add_two(n INT, x INT DEFAULT 1, y INT DEFAULT 1) RETURN n + x + y;
-- @expect: payload Create

-- @name: Multiple trailing arguments can use their defaults
SELECT add_two(1) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 3      |

-- @name: An explicit NULL does not use the default value
SELECT add_two(1, NULL, 2) AS r;
-- @expect:
-- | r    |
-- | ---- |
-- | NULL |

CREATE FUNCTION case_default(x INT DEFAULT CASE WHEN true THEN ABS(-2) ELSE 3 END) RETURN x;
-- @expect: payload Create

-- @name: Argument defaults can use CASE and built-in functions
SELECT case_default() AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 2      |

-- @name: A required argument cannot be omitted
SELECT add_one();
-- @expect: error Planner.FunctionArgsLengthNotWithinRange
-- @json:
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 0,
--   "name": "add_one"
-- }

-- @name: Extra arguments are rejected
SELECT add_one(1, 2, 4);
-- @expect: error Planner.FunctionArgsLengthNotWithinRange
-- @json:
-- {
--   "expected_maximum": 2,
--   "expected_minimum": 1,
--   "found": 3,
--   "name": "add_one"
-- }

-- @name: Required arguments must precede arguments with defaults
CREATE FUNCTION invalid_defaults(n INT, x INT DEFAULT 1, y INT) RETURN n + x + y;
-- @expect: error Planner.NonDefaultArgumentFollowsDefaultArgument

-- @name: Every argument must have a name
CREATE FUNCTION test(INT) RETURN 1;
-- @expect: error Translate.UnNamedFunctionArgNotSupported

-- @name: Argument names must be distinct
CREATE FUNCTION test(a INT, a BOOLEAN) RETURN 1;
-- @expect: error Planner.DuplicateArgName
-- @json: "a"

-- @name: Calling a custom function in an argument default is rejected
CREATE FUNCTION test(a INT DEFAULT test()) RETURN 1;
-- @expect: error Planner.CustomFunctionInArgumentDefault
-- @json: "TEST"

-- @name: An unselected default branch cannot contain a custom function call
CREATE FUNCTION test(a INT DEFAULT CASE WHEN true THEN 1 ELSE test() END) RETURN a;
-- @expect: error Planner.CustomFunctionInArgumentDefault
-- @json: "TEST"
