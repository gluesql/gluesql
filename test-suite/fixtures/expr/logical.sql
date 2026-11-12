-- @name: FALSE determines AND when the other operand is NULL
SELECT
    FALSE AND NULL AS false_and_null,
    NULL AND FALSE AS null_and_false;
-- @expect:
-- | false_and_null: Bool | null_and_false: Bool |
-- | -------------------- | -------------------- |
-- | false                | false                |

-- @name: TRUE determines OR when the other operand is NULL
SELECT
    TRUE OR NULL AS true_or_null,
    NULL OR TRUE AS null_or_true;
-- @expect:
-- | true_or_null: Bool | null_or_true: Bool |
-- | ------------------ | ------------------ |
-- | true               | true               |

-- @name: AND remains unknown when neither operand is FALSE
SELECT
    TRUE AND NULL AS true_and_null,
    NULL AND TRUE AS null_and_true,
    NULL AND NULL AS both_null;
-- @expect:
-- | true_and_null | null_and_true | both_null |
-- | ------------- | ------------- | --------- |
-- | NULL          | NULL          | NULL      |

-- @name: OR remains unknown when neither operand is TRUE
SELECT
    FALSE OR NULL AS false_or_null,
    NULL OR FALSE AS null_or_false,
    NULL OR NULL AS both_null;
-- @expect:
-- | false_or_null | null_or_false | both_null |
-- | ------------- | ------------- | --------- |
-- | NULL          | NULL          | NULL      |

-- @name: NOT and IS NULL consume the logical result
SELECT
    NOT (FALSE AND NULL) AS negated,
    (TRUE OR NULL) IS NULL AS is_null;
-- @expect:
-- | negated: Bool | is_null: Bool |
-- | ------------- | ------------- |
-- | true          | false         |
