-- @name: test length with string
SELECT LENGTH('Hello.');
-- @expect:
-- | LENGTH('Hello.'): U64 |
-- | --------------------- |
-- | 6                     |

-- @name: test length with list
SELECT LENGTH(CAST('[1, 2, 3]' AS LIST))
-- @expect:
-- | LENGTH(CAST('[1, 2, 3]' AS LIST)): U64 |
-- | -------------------------------------- |
-- | 3                                      |

-- @name: test length with map
SELECT LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP))
-- @expect:
-- | LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP)): U64 |
-- | ------------------------------------------------------------- |
-- | 4                                                             |

-- @name: test length string - wide chars 1
SELECT LENGTH('한글');
-- @expect:
-- | LENGTH('한글'): U64 |
-- | ----------------- |
-- | 2                 |

-- @name: test length string - wide chars 2
SELECT LENGTH('한글 abc');
-- @expect:
-- | LENGTH('한글 abc'): U64 |
-- | --------------------- |
-- | 6                     |

-- @name: test length string - wide chars 3
SELECT LENGTH('é');
-- @expect:
-- | LENGTH('é'): U64 |
-- | ---------------- |
-- | 1                |

-- @name: test length string - wide chars 4
SELECT LENGTH('🧑');
-- @expect:
-- | LENGTH('🧑'): U64 |
-- | ---------------- |
-- | 1                |

-- @name: test length string - wide chars 5
SELECT LENGTH('❤️');
-- @expect:
-- | LENGTH('❤️'): U64 |
-- | ----------------- |
-- | 2                 |

-- @name: test length string - wide chars 6
SELECT LENGTH('👩‍🔬');
-- @expect:
-- | LENGTH('👩‍🔬'): U64 |
-- | ------------------ |
-- | 3                  |
