CREATE TABLE mytable (
    id8 INT8,
    id INTEGER,
    rate FLOAT,
    dec  decimal,
    flag BOOLEAN,
    text TEXT,
    DOB  Date,
    Tm   Time,
    ival Interval,
    tstamp Timestamp,
    uid    Uuid,
    hash   Map,
    glist  List
);
-- expect: ok

Show columns from mytable
-- expect: payload ShowColumns
-- [
--   [
--     "id8",
--     "Int8"
--   ],
--   [
--     "id",
--     "Int"
--   ],
--   [
--     "rate",
--     "Float"
--   ],
--   [
--     "dec",
--     "Decimal"
--   ],
--   [
--     "flag",
--     "Boolean"
--   ],
--   [
--     "text",
--     "Text"
--   ],
--   [
--     "DOB",
--     "Date"
--   ],
--   [
--     "Tm",
--     "Time"
--   ],
--   [
--     "ival",
--     "Interval"
--   ],
--   [
--     "tstamp",
--     "Timestamp"
--   ],
--   [
--     "uid",
--     "Uuid"
--   ],
--   [
--     "hash",
--     "Map"
--   ],
--   [
--     "glist",
--     "List"
--   ]
-- ]

Show columns from mytable1
-- expect: error Execute.TableNotFound
-- "mytable1"
