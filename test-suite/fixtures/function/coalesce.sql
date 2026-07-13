SELECT COALESCE() AS coalesce
-- expect: error Evaluate.FunctionRequiresMoreArguments
-- {
--   "found": 0,
--   "function_name": "COALESCE",
--   "required_minimum": 1
-- }

SELECT COALESCE(NULL) AS coalesce
-- expect:
-- | coalesce |
-- | NULL     |

SELECT COALESCE(NULL, 42) AS coalesce
-- expect:
-- | coalesce: I64 |
-- | 42            |

SELECT COALESCE((SELECT NULL), (SELECT 42)) as coalesce
-- expect:
-- | coalesce: I64 |
-- | 42            |

SELECT COALESCE(
    COALESCE(NULL),
    COALESCE(NULL, 'Answer to the Ultimate Question of Life')
) as coalesce
-- expect:
-- | coalesce: Str                             |
-- | "Answer to the Ultimate Question of Life" |

SELECT COALESCE('Hitchhiker', NULL) AS coalesce
-- expect:
-- | coalesce: Str |
-- | "Hitchhiker"  |

SELECT COALESCE(NULL, NULL, NULL) AS coalesce
-- expect:
-- | coalesce |
-- | NULL     |

SELECT COALESCE(NULL, 42, 84) AS coalesce
-- expect:
-- | coalesce: I64 |
-- | 42            |

SELECT COALESCE(NULL, 1.23, 4.56) AS coalesce
-- expect:
-- | coalesce: F64 |
-- | 1.23          |

SELECT COALESCE(NULL, TRUE, FALSE) AS coalesce
-- expect:
-- | coalesce: Bool |
-- | true           |

SELECT COALESCE(NULL, COALESCE());
-- expect: error Evaluate.FunctionRequiresMoreArguments
-- {
--   "found": 0,
--   "function_name": "COALESCE",
--   "required_minimum": 1
-- }

CREATE TABLE TestCoalesce (
    id INTEGER,
    text_value TEXT NULL,
    integer_value INTEGER NULL,
    float_value FLOAT NULL,
    boolean_value BOOLEAN NULL
);
-- expect: ok

INSERT INTO TestCoalesce (id, text_value, integer_value, float_value, boolean_value) VALUES
    (1, 'Hitchhiker', NULL, NULL, NULL),
    (2, NULL, 42, NULL, NULL),
    (3, NULL, NULL, 1.11, NULL),
    (4, NULL, NULL, NULL, TRUE),
    (5, 'Universe', 84, 2.22, FALSE);
-- expect: ok

SELECT
    id,
    COALESCE(text_value, 'Default') AS coalesce_text,
    COALESCE(integer_value, 0) AS coalesce_integer,
    COALESCE(float_value, 0.1) AS coalesce_float,
    COALESCE(boolean_value, FALSE) AS coalesce_boolean
FROM TestCoalesce
ORDER BY id ASC
-- expect:
-- | id: I64 | coalesce_text: Str | coalesce_integer: I64 | coalesce_float: F64 | coalesce_boolean: Bool |
-- | 1       | "Hitchhiker"       | 0                     | 0.1                 | false                  |
-- | 2       | "Default"          | 42                    | 0.1                 | false                  |
-- | 3       | "Default"          | 0                     | 1.11                | false                  |
-- | 4       | "Default"          | 0                     | 0.1                 | true                   |
-- | 5       | "Universe"         | 84                    | 2.22                | false                  |

SELECT id, COALESCE(text_value, integer_value, float_value, boolean_value) AS coalesce FROM TestCoalesce ORDER BY id ASC
-- expect:
-- | id: I64 | coalesce          |
-- | 1       | Str("Hitchhiker") |
-- | 2       | I64(42)           |
-- | 3       | F64(1.11)         |
-- | 4       | Bool(true)        |
-- | 5       | Str("Universe")   |
