CREATE TABLE Test (
    id INTEGER NULL,
    num INTEGER NOT NULL,
    name TEXT
)
-- expect: ok

INSERT INTO Test (id, num, name) VALUES
    (NULL, 2, 'Hello'),
    (   1, 9, 'World'),
    (   3, 4, 'Great');
-- expect: ok

SELECT id, num, name FROM Test
-- expect:
-- | id: I64 | num: I64 | name: Str |
-- | NULL    | 2        | "Hello"   |
-- | 1       | 9        | "World"   |
-- | 3       | 4        | "Great"   |

SELECT id, num FROM Test WHERE id IS NULL AND name = 'Hello'
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE id IS NULL
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT name FROM Test WHERE SUBSTR(name, 1) IS NULL
-- expect:
-- | name |

SELECT id, num FROM Test WHERE id IS NOT NULL
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE id + 1 IS NULL
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE id + 1 IS NOT NULL
-- expect:
-- | id: I64 | num: I64 |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE 100 IS NULL
-- expect:
-- | id | num |

SELECT id, num FROM Test WHERE 100 IS NOT NULL
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE 8 + 3 IS NULL
-- expect:
-- | id | num |

SELECT id, num FROM Test WHERE 8 + 3 IS NOT NULL
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE NULL IS NULL
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE NULL IS NOT NULL
-- expect:
-- | id | num |

SELECT id, num FROM Test WHERE (NULL + id) IS NULL;
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE (NULL + NULL) IS NULL;
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE 'NULL' IS NULL
-- expect:
-- | id | num |

SELECT id, num FROM Test WHERE 'NULL' IS NOT NULL
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE (NULL + id) IS NULL;
-- expect:
-- | id: I64 | num: I64 |
-- | NULL    | 2        |
-- | 1       | 9        |
-- | 3       | 4        |

SELECT id, num FROM Test WHERE id + 1 IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE 1 + id IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE id - 1 IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE 1 - id IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE id * 1 IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE 1 * id IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE id / 1 IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id, num FROM Test WHERE 1 / id IS NULL;
-- expect:
-- | id   | num: I64 |
-- | NULL | 2        |

SELECT id + 1, 1 + id, id - 1, 1 - id, id * 1, 1 * id, id / 1, 1 / id FROM Test WHERE id IS NULL;
-- expect:
-- | id + 1 | 1 + id | id - 1 | 1 - id | id * 1 | 1 * id | id / 1 | 1 / id |
-- | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   | NULL   |

UPDATE Test SET id = 2
-- expect: ok

SELECT id FROM Test
-- expect:
-- | id: I64 |
-- | 2       |
-- | 2       |
-- | 2       |

SELECT id, num FROM Test
-- expect:
-- | id: I64 | num: I64 |
-- | 2       | 2        |
-- | 2       | 9        |
-- | 2       | 4        |

INSERT INTO Test VALUES (1, NULL, 'ok')
-- expect: error Value.NullValueOnNotNullField
