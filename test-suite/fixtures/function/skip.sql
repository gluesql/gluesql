CREATE TABLE Test (
    id INTEGER,
    list LIST
    )
-- @expect: ok

INSERT INTO Test (id, list) VALUES (1,'[1,2,3,4,5]')
-- @expect: ok

-- @name: skip function with normal usage
SELECT SKIP(list, 2) as col1 FROM Test
-- @expect:
-- | col1: List |
-- | [3,4,5]    |

-- @name: skip function with out of range index
SELECT SKIP(list, 6) as col1 FROM Test
-- @expect:
-- | col1: List |
-- | []         |

-- @name: skip function with null list
SELECT SKIP(NULL, 2) as col1 FROM Test
-- @expect:
-- | col1 |
-- | NULL |

-- @name: skip function with null size
SELECT SKIP(list, NULL) as col1 FROM Test
-- @expect:
-- | col1 |
-- | NULL |

-- @name: skip function with non integer parameter
SELECT SKIP(list, 'd') as col1 FROM Test
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "SKIP"

-- @name: skip function with non list
SELECT SKIP(id, 2) as col1 FROM Test
-- @expect: error Evaluate.ListTypeRequired

-- @name: skip function with negative size
SELECT SKIP(id, -2) as col1 FROM Test
-- @expect: error Evaluate.FunctionRequiresUSizeValue
-- @json: "SKIP"
