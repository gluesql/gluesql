VALUES(CONCAT_WS(',', 'AB', 'CD', 'EF'))
-- expect:
-- | column1: Str |
-- | "AB,CD,EF"   |

CREATE TABLE Concat (
    id INTEGER,
    flag BOOLEAN,
    text TEXT,
    null_value TEXT NULL
);
-- expect: ok

INSERT INTO Concat VALUES (1, TRUE, 'Foo', NULL);
-- expect: ok

select concat_ws('/', id, flag, null_value, text) as myc from Concat;
-- expect:
-- | myc: Str     |
-- | "1/TRUE/Foo" |

select concat_ws('', 'ab', 'cd') as myc from Concat;
-- expect:
-- | myc: Str |
-- | "abcd"   |

select concat_ws('', 'ab', 'cd', 'ef') as myconcat from Concat;
-- expect:
-- | myconcat: Str |
-- | "abcdef"      |

select concat_ws(',', 'ab', 'cd', 'ef') as myconcat from Concat;
-- expect:
-- | myconcat: Str |
-- | "ab,cd,ef"    |

select concat_ws('/', 'ab', 'cd', 'ef') as myconcat from Concat;
-- expect:
-- | myconcat: Str |
-- | "ab/cd/ef"    |

select concat_ws('', 'ab', 'cd', NULL, 'ef') as myconcat from Concat;
-- expect:
-- | myconcat: Str |
-- | "abcdef"      |

select concat_ws('', 123, 456, 3.14) as myconcat from Concat;
-- expect:
-- | myconcat: Str |
-- | "1234563.14"  |

select concat_ws() as myconcat from Concat;
-- expect: error Translate.FunctionArgsLengthNotMatchingMin
-- {
--   "expected_minimum": 2,
--   "found": 0,
--   "name": "CONCAT_WS"
-- }
