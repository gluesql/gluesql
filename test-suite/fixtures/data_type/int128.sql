CREATE TABLE Item (
    field_one INT128,
    field_two INT128
);
-- @expect: ok

INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);
-- @expect: ok

INSERT INTO Item VALUES (170141183460469231731687303715884105728, 170141183460469231731687303715884105728)
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int128",
--   "literal": "170141183460469231731687303715884105728"
-- }

INSERT INTO Item VALUES (-170141183460469231731687303715884105729, -170141183460469231731687303715884105729)
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int128",
--   "literal": "-170141183460469231731687303715884105729"
-- }

select cast(170141183460469231731687303715884105728 as INT128) from Item
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int128",
--   "literal": "170141183460469231731687303715884105728"
-- }

select cast(-170141183460469231731687303715884105729 as INT128) from Item
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int128",
--   "literal": "-170141183460469231731687303715884105729"
-- }

SELECT field_one, field_two FROM Item
-- @expect:
-- | field_one: I128 | field_two: I128 |
-- | 1               | -1              |
-- | -2              | 2               |
-- | 3               | 3               |
-- | -4              | -4              |

SELECT field_one FROM Item WHERE field_one = 1
-- @expect:
-- | field_one: I128 |
-- | 1               |

SELECT field_one FROM Item WHERE field_one > 0
-- @expect:
-- | field_one: I128 |
-- | 1               |
-- | 3               |

SELECT field_one FROM Item WHERE field_one >= 0
-- @expect:
-- | field_one: I128 |
-- | 1               |
-- | 3               |

SELECT field_one FROM Item WHERE field_one = -2
-- @expect:
-- | field_one: I128 |
-- | -2              |

SELECT field_one FROM Item WHERE field_one < 0
-- @expect:
-- | field_one: I128 |
-- | -2              |
-- | -4              |

SELECT field_one FROM Item WHERE field_one <= 0
-- @expect:
-- | field_one: I128 |
-- | -2              |
-- | -4              |

SELECT field_one + field_two AS plus FROM Item;
-- @expect:
-- | plus: I128 |
-- | 0          |
-- | 0          |
-- | 6          |
-- | -8         |

SELECT field_one - field_two AS sub FROM Item;
-- @expect:
-- | sub: I128 |
-- | 2         |
-- | -4        |
-- | 0         |
-- | 0         |

SELECT field_one * field_two AS mul FROM Item;
-- @expect:
-- | mul: I128 |
-- | -1        |
-- | -4        |
-- | 9         |
-- | 16        |

SELECT field_one / field_two AS div FROM Item;
-- @expect:
-- | div: I128 |
-- | -1        |
-- | -1        |
-- | 1         |
-- | 1         |

SELECT field_one % field_two AS modulo FROM Item;
-- @expect:
-- | modulo: I128 |
-- | 0            |
-- | 0            |
-- | 0            |
-- | 0            |

DELETE FROM Item
-- @expect: ok
