-- @name: PostgreSQL regular-expression operators use Rust regex syntax
VALUES
    ('Hello' ~ 'ell'),
    ('Hello' ~* '^hello$'),
    ('Hello' !~ '^hello$'),
    ('Hello' !~* '^hello$'),
    (NULL ~ 'x'),
    ('x' !~ NULL);
-- @expect:
-- | column1: Bool |
-- | ------------- |
-- | true          |
-- | true          |
-- | true          |
-- | false         |
-- | NULL          |
-- | NULL          |

CREATE TABLE RegexItem (name TEXT);
-- @expect: ok

INSERT INTO RegexItem VALUES ('Amelia'), ('Doll'), ('Gascoigne');
-- @expect: ok

SELECT name FROM RegexItem WHERE name ~* '^a|g';
-- @expect: count 2

VALUES (1 ~ 'x');
-- @expect: error Value.RegexOnNonString

VALUES (1 !~* 'x');
-- @expect: error Value.RegexOnNonString

-- a non-text operand is rejected even when the other side is NULL
VALUES (1 ~ NULL);
-- @expect: error Value.RegexOnNonString

VALUES ('x' ~ '[');
-- @expect: error StringExt.InvalidRegexPattern

VALUES ('x' ~* '[');
-- @expect: error StringExt.InvalidRegexPattern
