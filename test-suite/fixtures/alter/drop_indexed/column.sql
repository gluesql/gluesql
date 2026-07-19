CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello');
-- @expect: ok

CREATE INDEX idx_name ON Test (num + 1);
-- @expect: ok

CREATE INDEX idx_id ON Test (id);
-- @expect: ok

CREATE INDEX idx_typed_string ON Test ((id));
-- @expect: ok

CREATE INDEX idx_binary_op ON Test (id || name);
-- @expect: ok

CREATE INDEX idx_unary_op ON Test (-id);
-- @expect: ok

CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));
-- @expect: ok

CREATE INDEX idx_literal ON Test (100);
-- @expect: error Alter.IndexExprRequiresColumnReference

SELECT id, num, name FROM Test;
-- @expect-index: none
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE id <= 1;
-- @expect-index: idx_id <= 1
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE id <= (1);
-- @expect-index: idx_id <= (1)
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE id || name = '1Hello';
-- @expect-index: idx_binary_op = '1Hello'
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE -id >= -7;
-- @expect-index: idx_unary_op >= -7
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE -id > -7;
-- @expect-index: idx_unary_op > -7
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = '1';
-- @expect-index: idx_cast = '1'
-- @expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

ALTER TABLE Noname DROP COLUMN id;
-- @expect: error Alter.TableNotFound
-- @json: "Noname"

ALTER TABLE Test DROP COLUMN id;
-- @expect: ok

SELECT * FROM Test;
-- @expect-index: none
-- @expect:
-- | num: I64 | name: Str |
-- | 2        | "Hello"   |

SHOW INDEXES FROM Test;
-- @expect:
-- | TABLE_NAME: Str | INDEX_NAME: Str | ORDER: Str | EXPRESSION: Str | UNIQUENESS: Bool |
-- | "Test"          | "idx_name"      | "BOTH"     | "num + 1"       | false            |
