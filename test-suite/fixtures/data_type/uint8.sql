CREATE TABLE Item (
    field_one UINT8,
    field_two UINT8
);
-- @expect: ok

INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);
-- @expect: ok

INSERT INTO Item VALUES (256, 256);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Uint8",
--   "literal": "256"
-- }

INSERT INTO Item VALUES (-129, -129);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Uint8",
--   "literal": "-129"
-- }

SELECT field_one, field_two FROM Item
-- @expect:
-- | field_one: U8 | field_two: U8 |
-- | ------------- | ------------- |
-- | 1             | 1             |
-- | 2             | 2             |
-- | 3             | 3             |
-- | 4             | 4             |

SELECT field_one FROM Item WHERE field_one > 0
-- @expect:
-- | field_one: U8 |
-- | ------------- |
-- | 1             |
-- | 2             |
-- | 3             |
-- | 4             |

SELECT field_one FROM Item WHERE field_one >= 0
-- @expect:
-- | field_one: U8 |
-- | ------------- |
-- | 1             |
-- | 2             |
-- | 3             |
-- | 4             |

SELECT field_one FROM Item WHERE field_one = 2
-- @expect:
-- | field_one: U8 |
-- | ------------- |
-- | 2             |
