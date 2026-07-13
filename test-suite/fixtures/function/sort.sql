CREATE TABLE Test1 (list LIST)

-- expect: ok

INSERT INTO Test1 (list) VALUES ('[2, 1, 4, 3]')

-- expect: ok

-- name: sort the list by default order
SELECT SORT(list) AS list FROM Test1

-- expect:
-- | list: List |
-- | [1,2,3,4]  |

-- name: sort the list by ascending order
SELECT SORT(list, 'ASC') AS list FROM Test1

-- expect:
-- | list: List |
-- | [1,2,3,4]  |

-- name: sort the list by descending order
SELECT SORT(list, 'DESC') AS list FROM Test1

-- expect:
-- | list: List |
-- | [4,3,2,1]  |

-- name: sort the list by wrong order
SELECT SORT(list, 'WRONG') AS list FROM Test1

-- expect: error Evaluate.InvalidSortOrder

-- name: sort the list with not String typed order
SELECT SORT(list, 1) AS list FROM Test1

-- expect: error Evaluate.InvalidSortOrder

CREATE TABLE Test2 (id INTEGER, list LIST)

-- expect: ok

INSERT INTO Test2 (id, list) VALUES (1, '[2, "1", ["a", "b"], 3]')

-- expect: ok

-- name: sort non-LIST items
SELECT SORT(id) AS list FROM Test2

-- expect: error Evaluate.ListTypeRequired

-- name: sort the list with not comparable types
SELECT SORT(list) AS list FROM Test2

-- expect: error Evaluate.InvalidSortType
