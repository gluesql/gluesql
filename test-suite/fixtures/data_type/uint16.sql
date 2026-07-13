CREATE TABLE Item (
    field_one UINT16,
    field_two UINT16
);

-- expect: ok

INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);

-- expect: ok

INSERT INTO Item VALUES (327689,327689);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint16",
--   "literal": "327689"
-- }

INSERT INTO Item VALUES (-32769, -32769);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint16",
--   "literal": "-32769"
-- }

SELECT field_one, field_two FROM Item

-- expect:
-- | field_one: U16 | field_two: U16 |
-- | 1              | 1              |
-- | 2              | 2              |
-- | 3              | 3              |
-- | 4              | 4              |

SELECT field_one FROM Item WHERE field_one > 0

-- expect:
-- | field_one: U16 |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one >= 0

-- expect:
-- | field_one: U16 |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one = 2

-- expect:
-- | field_one: U16 |
-- | 2              |
