CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- expect: ok

INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello');
-- expect: ok

CREATE INDEX idx_id ON Test (id);
-- expect: payload CreateIndex

CREATE INDEX idx_typed_string ON Test ((id));
-- expect: payload CreateIndex

CREATE INDEX idx_binary_op ON Test (num || name);
-- expect: payload CreateIndex

CREATE INDEX idx_unary_op ON Test (-num);
-- expect: payload CreateIndex

CREATE INDEX idx_cast ON Test (CAST(id AS TEXT));
-- expect: payload CreateIndex

CREATE INDEX idx_literal ON Test (100);
-- expect: error Alter.IndexExprRequiresColumnReference

INSERT INTO Test VALUES (4, 7, 'Well');
-- expect: payload Insert
-- 1

SELECT id, num, name FROM Test;
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 4       | 7        | "Well"    |

SELECT id, num, name FROM Test WHERE id <= 1;
-- expect-index: idx_id <= 1
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE id <= (1);
-- expect-index: idx_id <= (1)
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE num || name = '2Hello';
-- expect-index: idx_binary_op = '2Hello'
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE (num || name) = '2Hello';
-- expect-index: idx_binary_op = '2Hello'
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE '7Well' = (num || name);
-- expect-index: idx_binary_op = '7Well'
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 4       | 7        | "Well"    |

SELECT id, num, name FROM Test WHERE -num < -2;
-- expect-index: idx_unary_op < -2
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 4       | 7        | "Well"    |

SELECT id, num, name FROM Test WHERE CAST(id AS TEXT) = '4';
-- expect-index: idx_cast = '4'
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 4       | 7        | "Well"    |
