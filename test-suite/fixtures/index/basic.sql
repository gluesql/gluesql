CREATE TABLE Test (
    id INTEGER,
    num INTEGER,
    name TEXT
);
-- expect: ok

INSERT INTO Test (id, num, name)
VALUES
    (1, 2, 'Hello'),
    (1, 17, 'World'),
    (11, 7, 'Great'),
    (4, 7, 'Job');
-- expect: ok

CREATE INDEX idx_id ON Test (id);
-- expect: payload CreateIndex

CREATE INDEX idx_name ON Test (name);
-- expect: payload CreateIndex

CREATE INDEX idx_id2 ON Test (id + num);
-- expect: payload CreateIndex

SELECT id, num, name FROM Test;
-- expect-index: none
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 11      | 7        | "Great"   |
-- | 4       | 7        | "Job"     |

SELECT id, num, name FROM Test WHERE id < 20;
-- expect-index: idx_id < 20
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 4       | 7        | "Job"     |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE 20 > id;
-- expect-index: idx_id < 20
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 4       | 7        | "Job"     |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE id <= 4;
-- expect-index: idx_id <= 4
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 4       | 7        | "Job"     |

SELECT id, num, name FROM Test WHERE 4 >= id;
-- expect-index: idx_id <= 4
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 4       | 7        | "Job"     |

SELECT id, num, name FROM Test WHERE id >= 4;
-- expect-index: idx_id >= 4
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 4       | 7        | "Job"     |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE 4 <= id;
-- expect-index: idx_id >= 4
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 4       | 7        | "Job"     |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE id > 0;
-- expect-index: idx_id > 0
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 4       | 7        | "Job"     |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE 4 < id;
-- expect-index: idx_id > 4
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 11      | 7        | "Great"   |

SELECT id, num, name FROM Test WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |

INSERT INTO Test (id, num, name) VALUES (1, 30, 'New one');
-- expect: payload Insert
-- 1

SELECT id, num, name FROM Test WHERE 1 = id;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |
-- | 1       | 17       | "World"   |
-- | 1       | 30       | "New one" |

SELECT id, num, name FROM Test WHERE name = 'New one';
-- expect-index: idx_name = 'New one'
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 30       | "New one" |

SELECT id, num, name FROM Test WHERE id + num = 10;
-- expect-index: idx_id2 = 10
-- expect:
-- | id: I64 | num: I64 | name: Str |

SELECT id, num, name FROM Test WHERE id + num < 11;
-- expect-index: idx_id2 < 11
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE 11 > id + num;
-- expect-index: idx_id2 < 11
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

SELECT id, num, name FROM Test WHERE id + num = 18;
-- expect-index: idx_id2 = 18
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 17       | "World"   |
-- | 11      | 7        | "Great"   |

DELETE FROM Test WHERE id = 11;
-- expect: payload Delete
-- 1

SELECT id, num, name FROM Test WHERE id + num = 3;
-- expect-index: idx_id2 = 3
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 1       | 2        | "Hello"   |

UPDATE Test SET id = id + 1 WHERE id = 1;
-- expect: payload Update
-- 3

SELECT * FROM Test WHERE 19 = id + num;
-- expect-index: idx_id2 = 19
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 2       | 17       | "World"   |

DROP INDEX Test.idx_id2;
-- expect: payload DropIndex

SELECT * FROM Test WHERE id + num = 19;
-- expect-index: none
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 2       | 17       | "World"   |

SELECT id FROM Test WHERE id + num = id;
-- expect-index: none
-- expect:
-- | id: I64 |

SELECT id, num, name FROM Test WHERE id < 20;
-- expect-index: idx_id < 20
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | 2       | 2        | "Hello"   |
-- | 2       | 17       | "World"   |
-- | 2       | 30       | "New one" |
-- | 4       | 7        | "Job"     |

CREATE INDEX idx_com ON Test (id, num);
-- expect: error Translate.CompositeIndexNotSupported

DROP INDEX Test.idx_id, Test.idx_id2;
-- expect: error Translate.TooManyParamsInDropIndex

CREATE INDEX idx_wow ON Test (a.b);
-- expect: error Alter.UnsupportedIndexExpr

CREATE INDEX idx_wow ON Abc (name);
-- expect: error Alter.TableNotFound
-- "Abc"

DROP INDEX NoNameTable.idx_id;
-- expect: error Index.TableNotFound
-- "NoNameTable"

CREATE INDEX idx_name ON Test (name || id);
-- expect: error Index.IndexNameAlreadyExists
-- "idx_name"

DROP INDEX Test.idx_aaa;
-- expect: error Index.IndexNameDoesNotExist
-- "idx_aaa"
