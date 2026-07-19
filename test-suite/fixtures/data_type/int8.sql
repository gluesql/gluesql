CREATE TABLE Item (
    field_one INT8,
    field_two INT8
);
-- @expect: ok

INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);
-- @expect: ok

INSERT INTO Item VALUES (128, 128);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int8",
--   "literal": "128"
-- }

INSERT INTO Item VALUES (-129, -129);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int8",
--   "literal": "-129"
-- }

SELECT field_one, field_two FROM Item
-- @expect:
-- | field_one: I8 | field_two: I8 |
-- | 1             | -1            |
-- | -2            | 2             |
-- | 3             | 3             |
-- | -4            | -4            |

SELECT field_one FROM Item WHERE field_one > 0
-- @expect:
-- | field_one: I8 |
-- | 1             |
-- | 3             |

SELECT field_one FROM Item WHERE field_one >= 0
-- @expect:
-- | field_one: I8 |
-- | 1             |
-- | 3             |

SELECT field_one FROM Item WHERE field_one = -2
-- @expect:
-- | field_one: I8 |
-- | -2            |

SELECT field_one FROM Item WHERE field_one < 0
-- @expect:
-- | field_one: I8 |
-- | -2            |
-- | -4            |

SELECT field_one FROM Item WHERE field_one <= 0
-- @expect:
-- | field_one: I8 |
-- | -2            |
-- | -4            |

SELECT field_one + field_two AS plus FROM Item;
-- @expect:
-- | plus: I8 |
-- | 0        |
-- | 0        |
-- | 6        |
-- | -8       |

SELECT field_one - field_two AS sub FROM Item;
-- @expect:
-- | sub: I8 |
-- | 2       |
-- | -4      |
-- | 0       |
-- | 0       |

SELECT field_one * field_two AS mul FROM Item;
-- @expect:
-- | mul: I8 |
-- | -1      |
-- | -4      |
-- | 9       |
-- | 16      |

SELECT field_one / field_two AS div FROM Item;
-- @expect:
-- | div: I8 |
-- | -1      |
-- | -1      |
-- | 1       |
-- | 1       |

SELECT field_one % field_two AS modulo FROM Item;
-- @expect:
-- | modulo: I8 |
-- | 0          |
-- | 0          |
-- | 0          |
-- | 0          |

DELETE FROM Item
-- @expect: ok
