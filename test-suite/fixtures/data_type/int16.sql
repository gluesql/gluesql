CREATE TABLE Item (
    field_one INT16,
    field_two INT16
);
-- @expect: ok

INSERT INTO Item VALUES (1, -1), (-2, 2), (3, 3), (-4, -4);
-- @expect: ok

INSERT INTO Item VALUES (32768, 32768);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int16",
--   "literal": "32768"
-- }

INSERT INTO Item VALUES (-32769, -32769);
-- @expect: error Evaluate.NumberParseFailed
-- @json:
-- {
--   "data_type": "Int16",
--   "literal": "-32769"
-- }

SELECT field_one, field_two FROM Item
-- @expect:
-- | field_one: I16 | field_two: I16 |
-- | 1              | -1             |
-- | -2             | 2              |
-- | 3              | 3              |
-- | -4             | -4             |

SELECT field_one FROM Item WHERE field_one > 0
-- @expect:
-- | field_one: I16 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one >= 0
-- @expect:
-- | field_one: I16 |
-- | 1              |
-- | 3              |

SELECT field_one FROM Item WHERE field_one = -2
-- @expect:
-- | field_one: I16 |
-- | -2             |

SELECT field_one FROM Item WHERE field_one < 0
-- @expect:
-- | field_one: I16 |
-- | -2             |
-- | -4             |

SELECT field_one FROM Item WHERE field_one <= 0
-- @expect:
-- | field_one: I16 |
-- | -2             |
-- | -4             |

SELECT field_one + field_two AS plus FROM Item;
-- @expect:
-- | plus: I16 |
-- | 0         |
-- | 0         |
-- | 6         |
-- | -8        |

SELECT field_one - field_two AS sub FROM Item;
-- @expect:
-- | sub: I16 |
-- | 2        |
-- | -4       |
-- | 0        |
-- | 0        |

SELECT field_one * field_two AS mul FROM Item;
-- @expect:
-- | mul: I16 |
-- | -1       |
-- | -4       |
-- | 9        |
-- | 16       |

SELECT field_one / field_two AS div FROM Item;
-- @expect:
-- | div: I16 |
-- | -1       |
-- | -1       |
-- | 1        |
-- | 1        |

SELECT field_one % field_two AS modulo FROM Item;
-- @expect:
-- | modulo: I16 |
-- | 0           |
-- | 0           |
-- | 0           |
-- | 0           |

DELETE FROM Item
-- @expect: ok
