CREATE TABLE computer (ip INET)
-- @expect: payload Create

INSERT INTO computer VALUES
    ('::1'),
    ('127.0.0.1'),
    ('0.0.0.0'),
    (4294967295),
    (9876543210);
-- @expect: payload Insert
-- @json: 5

SELECT * FROM computer
-- @expect:
-- | ip: Inet          |
-- | "::1"             |
-- | "127.0.0.1"       |
-- | "0.0.0.0"         |
-- | "255.255.255.255" |
-- | "::2:4cb0:16ea"   |

SELECT * FROM computer WHERE ip > '127.0.0.1'
-- @expect:
-- | ip: Inet          |
-- | "::1"             |
-- | "255.255.255.255" |
-- | "::2:4cb0:16ea"   |

SELECT * FROM computer WHERE ip = '127.0.0.1'
-- @expect:
-- | ip: Inet    |
-- | "127.0.0.1" |

INSERT INTO computer VALUES (0)
-- @expect: payload Insert
-- @json: 1

INSERT INTO computer VALUES ('127.0.0.0.1')
-- @expect: error Evaluate.TextParseFailed
-- @json:
-- {
--   "data_type": "Inet",
--   "literal": "127.0.0.0.1"
-- }
