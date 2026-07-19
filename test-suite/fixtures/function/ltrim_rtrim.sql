CREATE TABLE Item (name TEXT DEFAULT RTRIM(LTRIM('   abc   ')))
-- @expect: payload Create

INSERT INTO Item VALUES (' zzzytest'), ('testxxzx ')
-- @expect: payload Insert
-- @json: 2

SELECT LTRIM('x', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | ""        |
-- | ""        |

SELECT LTRIM('txu', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | "txu"     |
-- | "txu"     |

SELECT LTRIM(name) AS test FROM Item
-- @expect:
-- | test: Str   |
-- | "zzzytest"  |
-- | "testxxzx " |

SELECT LTRIM(RTRIM('GlueSQLABC', 'ABC')) AS test FROM Item;
-- @expect:
-- | test: Str |
-- | "GlueSQL" |
-- | "GlueSQL" |

SELECT LTRIM(name, ' xyz') AS test FROM Item
-- @expect:
-- | test: Str   |
-- | "test"      |
-- | "testxxzx " |

SELECT RTRIM(name) AS test FROM Item
-- @expect:
-- | test: Str   |
-- | " zzzytest" |
-- | "testxxzx"  |

SELECT RTRIM(name, 'xyz ') AS test FROM Item
-- @expect:
-- | test: Str   |
-- | " zzzytest" |
-- | "test"      |

SELECT RTRIM('x', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | ""        |
-- | ""        |

SELECT RTRIM('tuv', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | "tuv"     |
-- | "tuv"     |

SELECT RTRIM('txu', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | "txu"     |
-- | "txu"     |

SELECT RTRIM('xux', 'xyz') AS test from Item;
-- @expect:
-- | test: Str |
-- | "xu"      |
-- | "xu"      |

SELECT LTRIM(1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "LTRIM"

SELECT LTRIM(name, 1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "LTRIM"

SELECT RTRIM(1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "RTRIM"

SELECT RTRIM(name, 1) AS test FROM Item
-- @expect: error Evaluate.FunctionRequiresStringValue
-- @json: "RTRIM"

CREATE TABLE NullTest (name TEXT null)
-- @expect: payload Create

INSERT INTO NullTest VALUES (null)
-- @expect: payload Insert
-- @json: 1

SELECT LTRIM('name', NULL) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT LTRIM(name) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT RTRIM(name) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT RTRIM('name', NULL) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT LTRIM(NULL, '123') AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT LTRIM(name, NULL) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT RTRIM(NULL, '123') AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |

SELECT RTRIM(name, NULL) AS test FROM NullTest
-- @expect:
-- | test |
-- | NULL |
