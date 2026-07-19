SELECT GREATEST(1,6,9,7,0,10) AS goat;
-- @expect:
-- | goat: I64 |
-- | --------- |
-- | 10        |

SELECT GREATEST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat;
-- @expect:
-- | goat: F64 |
-- | --------- |
-- | 10.5      |

SELECT GREATEST('bibibik', 'babamba', 'melona') AS goat;
-- @expect:
-- | goat: Str |
-- | --------- |
-- | "melona"  |

SELECT GREATEST(
    DATE '2023-07-17',
    DATE '2022-07-17',
    DATE '2023-06-17',
    DATE '2024-07-17',
    DATE '2024-07-18') AS goat;
-- @expect:
-- | goat: Date   |
-- | ------------ |
-- | "2024-07-18" |

SELECT GREATEST() AS goat;
-- @expect: error Translate.FunctionArgsLengthNotMatchingMin
-- @json:
-- {
--   "expected_minimum": 2,
--   "found": 0,
--   "name": "GREATEST"
-- }

SELECT GREATEST(1, 2, 'bibibik') AS goat;
-- @expect: error Evaluate.NonComparableArgumentError
-- @json: "GREATEST"

SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat;
-- @expect: error Evaluate.NonComparableArgumentError
-- @json: "GREATEST"

SELECT GREATEST(NULL, NULL, NULL) AS goat;
-- @expect: error Evaluate.NonComparableArgumentError
-- @json: "GREATEST"

SELECT GREATEST(true, false) AS goat;
-- @expect:
-- | goat: Bool |
-- | ---------- |
-- | true       |
