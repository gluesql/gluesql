CREATE TABLE Item (
    name TEXT DEFAULT UPPER('abc'),
    opt_name TEXT NULL DEFAULT LOWER('ABC')
)
-- expect: payload Create

INSERT INTO Item VALUES ('abcd', 'efgi'), ('Abcd', NULL), ('ABCD', 'EfGi')
-- expect: payload Insert
-- 3

SELECT name FROM Item WHERE LOWER(name) = 'abcd';
-- expect:
-- | name: Str |
-- | "abcd"    |
-- | "Abcd"    |
-- | "ABCD"    |

SELECT LOWER(name), UPPER(name) FROM Item;
-- expect:
-- | LOWER(name): Str | UPPER(name): Str |
-- | "abcd"           | "ABCD"           |
-- | "abcd"           | "ABCD"           |
-- | "abcd"           | "ABCD"           |

SELECT
    LOWER('Abcd') as lower,
    UPPER('abCd') as upper
FROM Item LIMIT 1;
-- expect:
-- | lower: Str | upper: Str |
-- | "abcd"     | "ABCD"     |

SELECT LOWER(opt_name), UPPER(opt_name) FROM Item;
-- expect:
-- | LOWER(opt_name): Str | UPPER(opt_name): Str |
-- | "efgi"               | "EFGI"               |
-- | NULL                 | NULL                 |
-- | "efgi"               | "EFGI"               |

SELECT LOWER() FROM Item
-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "LOWER"
-- }

SELECT LOWER(1) FROM Item
-- expect: error Evaluate.FunctionRequiresStringValue
-- "LOWER"

SELECT LOWER(a => 2) FROM Item
-- expect: error Translate.NamedFunctionArgNotSupported
