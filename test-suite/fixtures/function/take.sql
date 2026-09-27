CREATE TABLE Take (
    items LIST
);
-- @expect: ok

INSERT INTO Take VALUES
    (TAKE(CAST('[1, 2, 3, 4, 5]' AS LIST), 5));
-- @expect: ok

select take(items, 0) as mygoodtake from Take;
-- @expect:
-- | mygoodtake: List |
-- | ---------------- |
-- | []               |

select take(items, 3) as mygoodtake from Take;
-- @expect:
-- | mygoodtake: List |
-- | ---------------- |
-- | [1,2,3]          |

select take(items, 5) as mygoodtake from Take;
-- @expect:
-- | mygoodtake: List |
-- | ---------------- |
-- | [1,2,3,4,5]      |

select take(items, 10) as mygoodtake from Take;
-- @expect:
-- | mygoodtake: List |
-- | ---------------- |
-- | [1,2,3,4,5]      |

select take(NULL, 3) as mynulltake from Take;
-- @expect:
-- | mynulltake |
-- | ---------- |
-- | NULL       |

select take(items, NULL) as mynulltake from Take;
-- @expect:
-- | mynulltake |
-- | ---------- |
-- | NULL       |

select take(items, -5) as mymistake from Take;
-- @expect: error Evaluate.FunctionRequiresUSizeValue
-- @json: "TAKE"

select take(items, 'TEST') as mymistake from Take;
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "TAKE"

select take(0, 3) as mymistake from Take;
-- @expect: error Evaluate.ListTypeRequired
