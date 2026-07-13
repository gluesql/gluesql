select concat('ab', 'cd') as myc;
-- expect:
-- | myc: Str |
-- | "abcd"   |

select concat('ab', 'cd', 'ef') as myconcat;
-- expect:
-- | myconcat: Str |
-- | "abcdef"      |

select concat('ab', 'cd', NULL, 'ef') as myconcat;
-- expect:
-- | myconcat |
-- | NULL     |

select concat(DATE '2020-06-11', DATE '2020-16-3') as myconcat;
-- expect: error Evaluate.TextParseFailed
-- {
--   "data_type": "Date",
--   "literal": "2020-16-3"
-- }

select concat(123, 456, 3.14) as myconcat;
-- expect:
-- | myconcat: Str |
-- | "1234563.14"  |

select concat() as myconcat;
-- expect: error Evaluate.EmptyArgNotAllowedInConcat

SELECT CONCAT(
        CAST('[1, 2, 3]' AS LIST),
        CAST('["one", "two", "three"]' AS LIST)
    ) AS myconcat;
-- expect:
-- | myconcat: List              |
-- | [1,2,3,"one","two","three"] |
