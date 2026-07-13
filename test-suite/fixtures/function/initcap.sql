CREATE TABLE Item (
    name TEXT DEFAULT 'abcd'
)

-- expect: payload Create

INSERT INTO Item VALUES
    ('h/i jk'),
    (NULL),
    ('H/I JK')

-- expect: payload Insert
-- 3

SELECT name FROM Item WHERE INITCAP(name) = 'H/I Jk';

-- expect:
-- | name: Str |
-- | "h/i jk"  |
-- | "H/I JK"  |

SELECT INITCAP(name) FROM Item;

-- expect:
-- | INITCAP(name): Str |
-- | "H/I Jk"           |
-- | NULL               |
-- | "H/I Jk"           |

SELECT INITCAP() FROM Item

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "INITCAP"
-- }

SELECT INITCAP(1) FROM Item

-- expect: error Evaluate.FunctionRequiresStringValue
-- "INITCAP"

SELECT INITCAP(a => 2) FROM Item

-- expect: error Translate.NamedFunctionArgNotSupported
