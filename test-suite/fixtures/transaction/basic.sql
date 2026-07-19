CREATE TABLE TxTest (
    id INTEGER,
    name TEXT
);
-- @expect: ok

INSERT INTO TxTest VALUES
    (1, 'Friday'),
    (2, 'Phone');
-- @expect: ok

BEGIN;
-- @expect: payload StartTransaction

INSERT INTO TxTest VALUES (3, 'New one');
-- @expect: payload Insert
-- @json: 1

ROLLBACK;
-- @expect: payload Rollback

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: payload StartTransaction

INSERT INTO TxTest VALUES (3, 'Vienna');
-- @expect: payload Insert
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

BEGIN;
-- @expect: payload StartTransaction

DELETE FROM TxTest WHERE id = 3;
-- @expect: payload Delete
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

ROLLBACK;
-- @expect: payload Rollback

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |
-- | 3       | "Vienna"  |

BEGIN;
-- @expect: payload StartTransaction

DELETE FROM TxTest WHERE id = 3;
-- @expect: payload Delete
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: payload StartTransaction

UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
-- @expect: payload Update
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |

ROLLBACK;
-- @expect: payload Rollback

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Friday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: payload StartTransaction

UPDATE TxTest SET name = 'Sunday' WHERE id = 1;
-- @expect: payload Update
-- @json: 1

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |

COMMIT;
-- @expect: payload Commit

SELECT id, name FROM TxTest
-- @expect:
-- | id: I64 | name: Str |
-- | 1       | "Sunday"  |
-- | 2       | "Phone"   |

BEGIN;
-- @expect: ok

SELECT * FROM TxTest;
-- @expect: ok

ROLLBACK;
-- @expect: ok

BEGIN;
-- @expect: ok

SELECT * FROM TxTest;
-- @expect: ok

COMMIT;
-- @expect: ok

BEGIN;
-- @expect: ok

COMMIT;
-- @expect: ok
