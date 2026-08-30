SELECT JSON_BUILD_ARRAY() AS result;
-- @expect:
-- | result: List |
-- | ------------ |
-- | []           |

SELECT JSON_BUILD_ARRAY(1) AS result;
-- @expect:
-- | result: List |
-- | ------------ |
-- | [1]          |

SELECT JSON_BUILD_ARRAY(1, 'GlueSQL', TRUE) AS result;
-- @expect:
-- | result: List       |
-- | ------------------ |
-- | [1,"GlueSQL",true] |

SELECT JSON_BUILD_ARRAY(1, NULL, 'test') AS result;
-- @expect:
-- | result: List    |
-- | --------------- |
-- | [1,null,"test"] |

SELECT JSON_BUILD_ARRAY(
    CAST('[1, 2]' AS LIST),
    CAST('{"name": "GlueSQL"}' AS MAP)
) AS result;
-- @expect:
-- | result: List               |
-- | -------------------------- |
-- | [[1,2],{"name":"GlueSQL"}] |
