CREATE TABLE Append (
    id INTEGER,
    items LIST,
    element INTEGER,
    element2 TEXT
);

-- expect: ok

INSERT INTO Append VALUES
    (1, '[1, 2, 3]', 4, 'Foo');

-- expect: ok

select append(items, element) as myappend from Append;

-- expect:
-- | myappend: List |
-- | [1,2,3,4]      |

select append(items, element2) as myappend from Append;

-- expect:
-- | myappend: List |
-- | [1,2,3,"Foo"]  |

select append(element, element2) as myappend from Append

-- expect: error Evaluate.ListTypeRequired

CREATE TABLE Foo (
    elements LIST
);

-- expect: payload Create

INSERT INTO Foo VALUES (APPEND(CAST('[1, 2, 3]' AS LIST), 4));

-- expect: ok

select elements as myappend from Foo;

-- expect:
-- | myappend: List |
-- | [1,2,3,4]      |
