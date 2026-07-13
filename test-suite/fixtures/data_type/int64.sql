CREATE TABLE Item (
    field_one INT,
    field_two INT
);

-- expect: ok

INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);

-- expect: ok

INSERT INTO Item VALUES (9223372036854775808, -9223372036854775809)

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "9223372036854775808"
-- }

select cast(9223372036854775808 as INT) from Item

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "9223372036854775808"
-- }

select cast(-9223372036854775809 as INT) from Item

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int",
--   "literal": "-9223372036854775809"
-- }

SELECT field_one, field_two FROM Item

-- expect:
-- | field_one: I64 | field_two: I64 |
-- | 1              | -1             |
-- | -2             | 2              |
-- | 3              | 3              |
-- | -4             | -4             |

SELECT field_one FROM Item WHERE field_one = 1

-- expect:
-- | field_one: I64 |
-- | 1              |

SELECT field_one FROM Item WHERE field_one > 0

-- expect:
-- | field_one: I64 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one >= 0

-- expect:
-- | field_one: I64 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one = -2

-- expect:
-- | field_one: I64 |
-- | -2             |

SELECT field_one FROM Item WHERE field_one < 0

-- expect:
-- | field_one: I64 |
-- | -2             |
-- | -4             |

SELECT field_one FROM Item WHERE field_one <= 0

-- expect:
-- | field_one: I64 |
-- | -2             |
-- | -4             |

SELECT field_one + field_two AS plus FROM Item;

-- expect:
-- | plus: I64 |
-- | 0         |
-- | 0         |
-- | 6         |
-- | -8        |

SELECT field_one - field_two AS sub FROM Item;

-- expect:
-- | sub: I64 |
-- | 2        |
-- | -4       |
-- | 0        |
-- | 0        |

SELECT field_one * field_two AS mul FROM Item;

-- expect:
-- | mul: I64 |
-- | -1       |
-- | -4       |
-- | 9        |
-- | 16       |

SELECT field_one / field_two AS div FROM Item;

-- expect:
-- | div: I64 |
-- | -1       |
-- | -1       |
-- | 1        |
-- | 1        |

SELECT field_one % field_two AS modulo FROM Item;

-- expect:
-- | modulo: I64 |
-- | 0           |
-- | 0           |
-- | 0           |
-- | 0           |

INSERT INTO Item VALUES (9223372036854775807, -9223372036854775808)

-- expect: payload Insert
-- 1

DELETE FROM Item

-- expect: ok
