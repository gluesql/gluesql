CREATE TABLE Prepend (
    id INTEGER,
    items LIST,
    element INTEGER,
    element2 TEXT
);
-- @expect: ok

INSERT INTO Prepend VALUES
    (1, '[1, 2, 3]',0, 'Foo');
-- @expect: ok

select prepend(items, element) as myprepend from Prepend;
-- @expect:
-- | myprepend: List |
-- | [0,1,2,3]       |

select prepend(items, element2) as myprepend from Prepend;
-- @expect:
-- | myprepend: List |
-- | ["Foo",1,2,3]   |

select prepend(element, element2) as myprepend from Prepend
-- @expect: error Evaluate.ListTypeRequired

CREATE TABLE Foo (
    elements LIST
);
-- @expect: payload Create

INSERT INTO Foo VALUES (PREPEND(CAST('[1, 2, 3]' AS LIST), 0));
-- @expect: ok

select elements as myprepend from Foo;
-- @expect:
-- | myprepend: List |
-- | [0,1,2,3]       |
