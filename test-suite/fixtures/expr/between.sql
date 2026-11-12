-- @name: '0 BETWEEN 0 AND 0' should return true
SELECT 0 BETWEEN 0 AND 0 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: '1 BETWEEN 1 AND 1' should return true
SELECT 1 BETWEEN 1 AND 1 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: '2 BETWEEN 1 AND 3' should return true
SELECT 2 BETWEEN 1 AND 3 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: '-1 BETWEEN -1 AND 1' should return true
SELECT -1 BETWEEN -1 AND 1 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: '1 BETWEEN 2 AND 3' should return false
SELECT 1 BETWEEN 2 AND 3 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |

-- @name: '-170141183460469231731687303715884105728 BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727' should return true
SELECT -170141183460469231731687303715884105728 BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: '170141183460469231731687303715884105727 BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727' should return true
SELECT 170141183460469231731687303715884105727 BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | true      |

-- @name: 'NULL BETWEEN ...' should return NULL
SELECT (NULL BETWEEN -170141183460469231731687303715884105728 AND 170141183460469231731687303715884105727) as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: 'NULL BETWEEN NULL AND NULL' should return NULL
SELECT (NULL BETWEEN NULL AND NULL) as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '1 BETWEEN NULL AND 1' should return NULL
SELECT 1 BETWEEN NULL AND 1 as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '0 BETWEEN NULL AND 0' should return NULL
SELECT 0 BETWEEN NULL AND 0 as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '-1 BETWEEN NULL AND -1' should return NULL
SELECT -1 BETWEEN NULL AND -1 as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '1 BETWEEN NULL AND 2' should return NULL
SELECT 1 BETWEEN NULL AND 2 as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '-170141183460469231731687303715884105728 BETWEEN NULL AND 170141183460469231731687303715884105727' should return NULL
SELECT -170141183460469231731687303715884105728 BETWEEN NULL AND 170141183460469231731687303715884105727 as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '2 BETWEEN NULL AND 1' should return false
SELECT 2 BETWEEN NULL AND 1 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |

-- @name: '170141183460469231731687303715884105727 BETWEEN NULL AND -170141183460469231731687303715884105728' should return false
SELECT 170141183460469231731687303715884105727 BETWEEN NULL AND -170141183460469231731687303715884105728 as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |

-- @name: '1 BETWEEN 1 AND NULL' should return NULL
SELECT 1 BETWEEN 1 AND NULL as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '0 BETWEEN 0 AND NULL' should return NULL
SELECT 0 BETWEEN 0 AND NULL as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '-1 BETWEEN -1 AND NULL' should return NULL
SELECT -1 BETWEEN -1 AND NULL as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '1 BETWEEN 2 AND NULL' should return false
SELECT 1 BETWEEN 2 AND NULL as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |

-- @name: '-170141183460469231731687303715884105728 BETWEEN 170141183460469231731687303715884105727 AND NULL' should return false
SELECT -170141183460469231731687303715884105728 BETWEEN 170141183460469231731687303715884105727 AND NULL as res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |

-- @name: '2 BETWEEN 1 AND NULL' should return NULL
SELECT 2 BETWEEN 1 AND NULL as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: '170141183460469231731687303715884105727 BETWEEN -170141183460469231731687303715884105728 AND NULL' should return NULL
SELECT 170141183460469231731687303715884105727 BETWEEN -170141183460469231731687303715884105728 AND NULL as res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: NOT BETWEEN returns TRUE when either known bound excludes the value
SELECT
    0 NOT BETWEEN 1 AND NULL AS below_lower,
    2 NOT BETWEEN NULL AND 1 AS above_upper;
-- @expect:
-- | below_lower: Bool | above_upper: Bool |
-- | ----------------- | ----------------- |
-- | true              | true              |

-- @name: NOT BETWEEN preserves an unknown result
SELECT 1 NOT BETWEEN 0 AND NULL AS res;
-- @expect:
-- | res  |
-- | ---- |
-- | NULL |

-- @name: NOT BETWEEN returns FALSE for a value inside known bounds
SELECT 1 NOT BETWEEN 0 AND 2 AS res;
-- @expect:
-- | res: Bool |
-- | --------- |
-- | false     |
