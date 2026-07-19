CREATE TABLE A
-- @expect: ok

CREATE TABLE B
-- @expect: ok

CREATE TABLE S (a_id INTEGER, tag TEXT);
-- @expect: ok

INSERT INTO A VALUES ('{"a_id":1,"a":"left"}');
-- @expect: ok

INSERT INTO B VALUES ('{"b_id":10,"a_id":1,"b":"right"}');
-- @expect: ok

INSERT INTO S VALUES (1, 'schema');
-- @expect: ok

-- @name: schemaless wildcard projection on single table
SELECT * FROM A
-- @expect: maps
-- | {"a":"left","a_id":1} |

-- @name: schemaless qualified wildcard projection on single table
SELECT A.* FROM A
-- @expect: maps
-- | {"a":"left","a_id":1} |

-- @name: schemaless wildcard projection with root alias
SELECT * FROM A AS P
-- @expect: maps
-- | {"a":"left","a_id":1} |

-- @name: schemaless qualified wildcard projection with root alias
SELECT P.* FROM A AS P
-- @expect: maps
-- | {"a":"left","a_id":1} |

-- @name: schemaless projection by explicit fields in join
SELECT A.a_id AS a_id, B.b_id AS b_id FROM A JOIN B WHERE A.a_id = B.a_id
-- @expect:
-- | a_id: I64 | b_id: I64 |
-- | 1         | 10        |

-- @name: schemaless qualified wildcard keeps tabular output in join
SELECT B.* FROM A JOIN B WHERE A.a_id = B.a_id
-- @expect:
-- | _doc: Map                        |
-- | {"a_id":1,"b":"right","b_id":10} |

-- @name: schemaless qualified wildcard keeps table side in join
SELECT A.*, B.* FROM A JOIN B WHERE A.a_id = B.a_id
-- @expect:
-- | _doc: Map             | _doc: Map                        |
-- | {"a":"left","a_id":1} | {"a_id":1,"b":"right","b_id":10} |

-- @name: wildcard join with schemaless root and schemaful join is rejected
SELECT * FROM A JOIN S WHERE A.a_id = S.a_id
-- @expect: error Plan.SchemalessMixedJoinWildcardProjection

-- @name: wildcard join with schemaful root and schemaless join is rejected
SELECT * FROM S JOIN A WHERE S.a_id = A.a_id
-- @expect: error Plan.SchemalessMixedJoinWildcardProjection

CREATE TABLE C (_doc INTEGER);
-- @expect: ok

INSERT INTO C VALUES (7);
-- @expect: ok

-- @name: schemaful _doc column stays tabular
SELECT _doc FROM C
-- @expect:
-- | _doc: I64 |
-- | 7         |
