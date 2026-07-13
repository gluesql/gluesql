CREATE TABLE Item (
    field_one UINT32,
    field_two UINT32
);

-- expect: ok

INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);

-- expect: ok

INSERT INTO Item VALUES (4294967296,4294967296);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint32",
--   "literal": "4294967296"
-- }

INSERT INTO Item VALUES (-32769, -32769);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint32",
--   "literal": "-32769"
-- }

SELECT field_one, field_two FROM Item

-- expect:
-- | field_one: U32 | field_two: U32 |
-- | 1              | 1              |
-- | 2              | 2              |
-- | 3              | 3              |
-- | 4              | 4              |

SELECT field_one FROM Item WHERE field_one > 0

-- expect:
-- | field_one: U32 |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one >= 0

-- expect:
-- | field_one: U32 |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one = 2

-- expect:
-- | field_one: U32 |
-- | 2              |
