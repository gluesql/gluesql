-- @name: Define a function without arguments
CREATE FUNCTION add_none() RETURN NULL;
-- @expect: payload Create

-- @name: Call a function that returns NULL
SELECT add_none() AS r;
-- @expect:
-- | r    |
-- | ---- |
-- | NULL |

-- @name: CREATE OR REPLACE also creates a function when the name is absent
CREATE OR REPLACE FUNCTION add_zero(n INT) RETURN n;
-- @expect: payload Create

-- @name: The function body can return its argument
SELECT add_zero(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 4      |

-- @name: Replace an existing function definition
CREATE OR REPLACE FUNCTION add_zero(n INT) RETURN n + 1;
-- @expect: payload Create

-- @name: Subsequent calls use the replacement body
SELECT add_zero(4) AS r;
-- @expect:
-- | r: I64 |
-- | ------ |
-- | 5      |

-- @name: CREATE without OR REPLACE rejects an existing name
CREATE FUNCTION add_zero(n INT) RETURN n;
-- @expect: error Alter.FunctionAlreadyExists
-- @json: "add_zero"

-- @name: A function definition must have a body
CREATE FUNCTION missing_body();
-- @expect: error Translate.UnsupportedEmptyFunctionBody

CREATE FUNCTION add_one(n INT, x INT DEFAULT 1) RETURN n + x;
-- @expect: payload Create

CREATE FUNCTION add_two(n INT, x INT DEFAULT 1, y INT DEFAULT 1) RETURN n + x + y;
-- @expect: payload Create

-- @name: Drop an existing function
DROP FUNCTION add_none;
-- @expect: payload DropFunction

-- @name: SHOW FUNCTIONS lists the remaining functions and their argument types
SHOW FUNCTIONS;
-- @expect: payload ShowVariable.Functions
-- @json:
-- [
--   "add_one(n: INT, x: INT)",
--   "add_two(n: INT, x: INT, y: INT)",
--   "add_zero(n: INT)"
-- ]

-- @name: Dropping a missing function reports its name
DROP FUNCTION add_none;
-- @expect: error Alter.FunctionNotFound
-- @json: "add_none"

-- @name: Drop multiple functions with IF EXISTS
DROP FUNCTION IF EXISTS add_zero, add_one, add_two;
-- @expect: payload DropFunction
