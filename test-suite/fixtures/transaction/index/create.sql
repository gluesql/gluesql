CREATE TABLE IdxCreate (id INTEGER);
-- expect: ok

INSERT INTO IdxCreate VALUES (1);
-- expect: ok

BEGIN;
-- expect: ok

CREATE INDEX idx_id ON IdxCreate (id);
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 |
-- | 1       |

ROLLBACK;
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 1;
-- expect-index: none
-- expect:
-- | id: I64 |
-- | 1       |

BEGIN;
-- expect: ok

CREATE INDEX idx_id ON IdxCreate (id);
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 |
-- | 1       |

COMMIT;
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 1;
-- expect-index: idx_id = 1
-- expect:
-- | id: I64 |
-- | 1       |

DELETE FROM IdxCreate;
-- expect: ok

INSERT INTO IdxCreate VALUES (3);
-- expect: ok

BEGIN;
-- expect: ok

CREATE INDEX idx_id2 ON IdxCreate (id * 2);
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 3;
-- expect-index: idx_id = 3
-- expect:
-- | id: I64 |
-- | 3       |

SELECT id FROM IdxCreate WHERE id * 2 = 6;
-- expect-index: idx_id2 = 6
-- expect:
-- | id: I64 |
-- | 3       |

ROLLBACK;
-- expect: ok

SELECT id FROM IdxCreate WHERE id = 3;
-- expect-index: idx_id = 3
-- expect:
-- | id: I64 |
-- | 3       |

SELECT id FROM IdxCreate WHERE id * 2 = 6;
-- expect-index: none
-- expect:
-- | id: I64 |
-- | 3       |
