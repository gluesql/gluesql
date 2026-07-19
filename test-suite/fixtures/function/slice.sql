CREATE TABLE Test (list LIST)
-- @expect: ok

INSERT INTO Test VALUES ('[1,2,3,4]')
-- @expect: ok

-- @name: slice start in index 0
SELECT SLICE(list, 0, 2) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [1,2]       |

-- @name: slice with size
SELECT SLICE(list, 0, 4) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [1,2,3,4]   |

-- @name: slice with size that pass over array size
SELECT SLICE(list, 2, 5) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [3,4]       |

-- @name: slice that over array size
SELECT SLICE(list, 100, 5) AS value FROM Test;
-- @expect:
-- | value: List |
-- | []          |

-- @name: list value should be List Value
SELECT SLICE(1, 2, 2) AS value FROM Test;
-- @expect: error Evaluate.ListTypeRequired

-- @name: start value should be Integer Value
SELECT SLICE(list, 'b', 5) AS value FROM Test;
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "SLICE"

-- @name: start value should be Positive USIZE Value
SELECT SLICE(list, -1, 1) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [4]         |

-- @name: start value should be Positive USIZE Value
SELECT SLICE(list, -2, 4) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [3,4]       |

-- @name: start value should be Positive USIZE Value
SELECT SLICE(list, 9999, 4) AS value FROM Test;
-- @expect:
-- | value: List |
-- | []          |

-- @name: start value should be Positive USIZE Value
SELECT SLICE(list, 0, 1234) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [1,2,3,4]   |

-- @name: if absoulte value of negative index over length of list, covert to index 0
SELECT SLICE(list, -234, 4) AS value FROM Test;
-- @expect:
-- | value: List |
-- | [1,2,3,4]   |

-- @name: length value should be Integer Value
SELECT SLICE(list, 2, 'a') AS value FROM Test;
-- @expect: error Evaluate.FunctionRequiresIntegerValue
-- @json: "SLICE"
