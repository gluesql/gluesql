VALUES(ASCII('A'))

-- expect:
-- | column1: U8 |
-- | 65          |

VALUES(ASCII('AB'))

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue

CREATE TABLE Ascii (
    id INTEGER,
    text TEXT
);

-- expect: ok

INSERT INTO Ascii VALUES (1, 'F');

-- expect: ok

select ascii(text) as ascii from Ascii;

-- expect:
-- | ascii: U8 |
-- | 70        |

select ascii('a') as ascii from Ascii;

-- expect:
-- | ascii: U8 |
-- | 97        |

select ascii('A') as ascii from Ascii;

-- expect:
-- | ascii: U8 |
-- | 65        |

select ascii('ab') as ascii from Ascii;

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue

select ascii('AB') as ascii from Ascii;

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue

select ascii('') as ascii from Ascii;

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue

select ascii('ukjhg') as ascii from Ascii;

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue

select ascii(NULL) as ascii from Ascii;

-- expect:
-- | ascii |
-- | NULL  |

select ascii('ㄱ') as ascii from Ascii;

-- expect: error Evaluate.NonAsciiCharacterNotAllowed

select ascii() as ascii from Ascii;

-- expect: error Translate.FunctionArgsLengthNotMatching
-- {
--   "expected": 1,
--   "found": 0,
--   "name": "ASCII"
-- }

INSERT INTO Ascii VALUES (1, 'Foo');

-- expect: ok

select ascii(text) as ascii from Ascii;

-- expect: error Evaluate.AsciiFunctionRequiresSingleCharacterValue
