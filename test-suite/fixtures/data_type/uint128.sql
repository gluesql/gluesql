CREATE TABLE Item (
    field_one UINT128,
    field_two UINT128
);

-- expect: ok

INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4);

-- expect: ok

INSERT INTO Item VALUES (340282366920938463463374607431768211456,340282366920938463463374607431768211456);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint128",
--   "literal": "340282366920938463463374607431768211456"
-- }

INSERT INTO Item VALUES (-32769, -32769);

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Uint128",
--   "literal": "-32769"
-- }

SELECT field_one, field_two FROM Item

-- expect:
-- | field_one: U128 | field_two: U128 |
-- | 1               | 1               |
-- | 2               | 2               |
-- | 3               | 3               |
-- | 4               | 4               |

SELECT field_one FROM Item WHERE field_one > 0

-- expect:
-- | field_one: U128 |
-- | 1               |
-- | 2               |
-- | 3               |
-- | 4               |

SELECT field_one FROM Item WHERE field_one >= 0

-- expect:
-- | field_one: U128 |
-- | 1               |
-- | 2               |
-- | 3               |
-- | 4               |

SELECT field_one FROM Item WHERE field_one = 2

-- expect:
-- | field_one: U128 |
-- | 2               |
