CREATE TABLE Item (
    field_one UINT64,
    field_two UINT64
);
-- @expect: ok

INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);
-- @expect: ok

INSERT INTO Item VALUES (18446744073709551616,18446744073709551616);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Uint64",
--   "literal": "18446744073709551616"
-- }

INSERT INTO Item VALUES (-32769, -32769);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Uint64",
--   "literal": "-32769"
-- }

SELECT field_one, field_two FROM Item
-- @expect:
-- | field_one: U64 | field_two: U64 |
-- | -------------- | -------------- |
-- | 1              | 1              |
-- | 2              | 2              |
-- | 3              | 3              |
-- | 4              | 4              |

SELECT field_one FROM Item WHERE field_one > 0
-- @expect:
-- | field_one: U64 |
-- | -------------- |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one >= 0
-- @expect:
-- | field_one: U64 |
-- | -------------- |
-- | 1              |
-- | 2              |
-- | 3              |
-- | 4              |

SELECT field_one FROM Item WHERE field_one = 2
-- @expect:
-- | field_one: U64 |
-- | -------------- |
-- | 2              |
