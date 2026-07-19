VALUES(CHR(70))
-- @expect:
-- | column1: Str |
-- | "F"          |

VALUES(CHR(7070))
-- @expect: error Evaluate.ChrFunctionRequiresIntegerValueInRange0To255

CREATE TABLE Chr (
    id INTEGER,
    num INTEGER
);
-- @expect: ok

INSERT INTO Chr VALUES (1, 70);
-- @expect: ok

select chr(num) as chr from Chr;
-- @expect:
-- | chr: Str |
-- | "F"      |

select chr(65) as chr from Chr;
-- @expect:
-- | chr: Str |
-- | "A"      |

select chr(532) as chr from Chr;
-- @expect: error Evaluate.ChrFunctionRequiresIntegerValueInRange0To255

select chr('ukjhg') as chr from Chr;
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "CHR"

INSERT INTO Chr VALUES (1, 4345);
-- @expect: ok

select chr(num) as chr from Chr;
-- @expect: error Evaluate.ChrFunctionRequiresIntegerValueInRange0To255
