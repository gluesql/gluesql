CREATE TABLE Item (
    field_one INT32,
    field_two INT32
);

-- expect: ok

INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);

-- expect: ok

INSERT INTO Item VALUES (2147483648, -2147483649)

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int32",
--   "literal": "2147483648"
-- }

select cast(2147483648 as INT32) from Item

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int32",
--   "literal": "2147483648"
-- }

select cast(-2147483649 as INT32) from Item

-- expect: error Evaluate.NumberParseFailed
-- {
--   "data_type": "Int32",
--   "literal": "-2147483649"
-- }

SELECT field_one, field_two FROM Item

-- expect:
-- | field_one: I32 | field_two: I32 |
-- | 1              | -1             |
-- | -2             | 2              |
-- | 3              | 3              |
-- | -4             | -4             |

SELECT field_one FROM Item WHERE field_one = 1

-- expect:
-- | field_one: I32 |
-- | 1              |

SELECT field_one FROM Item WHERE field_one > 0

-- expect:
-- | field_one: I32 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one >= 0

-- expect:
-- | field_one: I32 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one = -2

-- expect:
-- | field_one: I32 |
-- | -2             |

SELECT field_one FROM Item WHERE field_one < 0

-- expect:
-- | field_one: I32 |
-- | -2             |
-- | -4             |

SELECT field_one FROM Item WHERE field_one <= 0

-- expect:
-- | field_one: I32 |
-- | -2             |
-- | -4             |

SELECT field_one + field_two AS plus FROM Item;

-- expect:
-- | plus: I32 |
-- | 0         |
-- | 0         |
-- | 6         |
-- | -8        |

SELECT field_one - field_two AS sub FROM Item;

-- expect:
-- | sub: I32 |
-- | 2        |
-- | -4       |
-- | 0        |
-- | 0        |

SELECT field_one * field_two AS mul FROM Item;

-- expect:
-- | mul: I32 |
-- | -1       |
-- | -4       |
-- | 9        |
-- | 16       |

SELECT field_one / field_two AS div FROM Item;

-- expect:
-- | div: I32 |
-- | -1       |
-- | -1       |
-- | 1        |
-- | 1        |

SELECT field_one % field_two AS modulo FROM Item;

-- expect:
-- | modulo: I32 |
-- | 0           |
-- | 0           |
-- | 0           |
-- | 0           |

INSERT INTO Item VALUES (2147483647, -2147483648)

-- expect: payload Insert
-- 1

DELETE FROM Item

-- expect: ok
