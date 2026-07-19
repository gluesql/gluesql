CREATE TABLE Item (
    name TEXT DEFAULT TRIM(LEADING 'a' FROM 'aabc') || TRIM('   good  ')
)
-- @expect: payload Create

INSERT INTO Item VALUES
    ('      Left blank'),
    ('Right blank     '),
    ('     Blank!     '),
    ('Not Blank');
-- @expect: payload Insert
-- @json: 4

SELECT TRIM(name) FROM Item;
-- @expect:
-- | TRIM(name): Str |
-- | --------------- |
-- | "Left blank"    |
-- | "Right blank"   |
-- | "Blank!"        |
-- | "Not Blank"     |

SELECT TRIM(1) FROM Item;
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "TRIM"

CREATE TABLE NullName (name TEXT NULL)
-- @expect: payload Create

INSERT INTO NullName VALUES (NULL)
-- @expect: payload Insert
-- @json: 1

SELECT TRIM(name) AS test FROM NullName;
-- @expect:
-- | test |
-- | ---- |
-- | NULL |

SELECT TRIM(BOTH NULL FROM name) FROM NullName;
-- @expect:
-- | TRIM(BOTH NULL FROM name) |
-- | ------------------------- |
-- | NULL                      |

SELECT TRIM(BOTH NULL FROM 'name') AS test
-- @expect:
-- | test |
-- | ---- |
-- | NULL |

SELECT TRIM(TRAILING NULL FROM name) FROM NullName;
-- @expect:
-- | TRIM(TRAILING NULL FROM name) |
-- | ----------------------------- |
-- | NULL                          |

SELECT TRIM(LEADING NULL FROM name) FROM NullName;
-- @expect:
-- | TRIM(LEADING NULL FROM name) |
-- | ---------------------------- |
-- | NULL                         |

CREATE TABLE Test (name TEXT)
-- @expect: payload Create

INSERT INTO Test VALUES
    ('     blank     '),
    ('xxxyzblankxyzxx'),
    ('xxxyzblank     '),
    ('     blankxyzxx'),
    ('  xyzblankxyzxx'),
    ('xxxyzblankxyz  ');
-- @expect: payload Insert
-- @json: 6

SELECT TRIM(BOTH 'xyz' FROM name) FROM Test;
-- @expect:
-- | TRIM(BOTH 'xyz' FROM name): Str |
-- | ------------------------------- |
-- | "     blank     "               |
-- | "blank"                         |
-- | "blank     "                    |
-- | "     blank"                    |
-- | "  xyzblank"                    |
-- | "blankxyz  "                    |

SELECT TRIM(LEADING 'xyz' FROM name) FROM Test;
-- @expect:
-- | TRIM(LEADING 'xyz' FROM name): Str |
-- | ---------------------------------- |
-- | "     blank     "                  |
-- | "blankxyzxx"                       |
-- | "blank     "                       |
-- | "     blankxyzxx"                  |
-- | "  xyzblankxyzxx"                  |
-- | "blankxyz  "                       |

SELECT TRIM(TRAILING 'xyz' FROM name) FROM Test;
-- @expect:
-- | TRIM(TRAILING 'xyz' FROM name): Str |
-- | ----------------------------------- |
-- | "     blank     "                   |
-- | "xxxyzblank"                        |
-- | "xxxyzblank     "                   |
-- | "     blank"                        |
-- | "  xyzblank"                        |
-- | "xxxyzblankxyz  "                   |

SELECT
    TRIM(BOTH '  hello  ') AS both,
    TRIM(LEADING '  hello  ') AS leading,
    TRIM(TRAILING '  hello  ') AS trailing
-- @expect:
-- | both: Str | leading: Str | trailing: Str |
-- | --------- | ------------ | ------------- |
-- | "hello"   | "hello  "    | "  hello"     |

SELECT
    TRIM(BOTH TRIM(BOTH ' potato ')) AS Case1,
    TRIM('xyz' FROM 'x') AS Case2,
    TRIM(TRAILING 'xyz' FROM 'xx') AS Case3
-- @expect:
-- | Case1: Str | Case2: Str | Case3: Str |
-- | ---------- | ---------- | ---------- |
-- | "potato"   | ""         | ""         |

SELECT TRIM('1' FROM 1) AS test FROM Test
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "TRIM"

SELECT TRIM(1 FROM TRIM('t' FROM 'tartare')) AS test FROM Test
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "TRIM"
