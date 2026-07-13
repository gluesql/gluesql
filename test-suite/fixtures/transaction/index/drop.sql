CREATE TABLE IdxDrop (id INTEGER);
-- expect: ok

INSERT INTO IdxDrop VALUES (1);
-- expect: ok

CREATE INDEX idx_id ON IdxDrop (id);
-- expect: ok

BEGIN;
-- expect: ok

DROP INDEX IdxDrop.idx_id;
-- expect: ok

SELECT id FROM IdxDrop WHERE id = 1;
-- expect-index: none
-- expect:
-- | id: I64 |
-- | 1       |

ROLLBACK;
-- expect: ok

SELECT id FROM IdxDrop WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 |
-- | 1       |

BEGIN;
-- expect: ok

DROP INDEX IdxDrop.idx_id;
-- expect: ok

SELECT id FROM IdxDrop WHERE id = 1;
-- expect-index: none
-- expect:
-- | id: I64 |
-- | 1       |

COMMIT;
-- expect: ok

SELECT id FROM IdxDrop WHERE id = 1;
-- expect-index: none
-- expect:
-- | id: I64 |
-- | 1       |
