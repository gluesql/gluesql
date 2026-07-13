-- name: 'NULL IS NULL' should return true
SELECT NULL IS NULL as res;

-- expect:
-- | res: Bool |
-- | true      |

-- name: 'NULL = NULL' should return NULL
SELECT NULL = NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL > NULL' should return NULL
SELECT NULL > NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL < NULL' should return NULL
SELECT NULL < NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL >= NULL' should return NULL
SELECT NULL >= NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL <= NULL' should return NULL
SELECT NULL <= NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL <> NULL' should return NULL
SELECT NULL <> NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL & NULL' should return NULL
SELECT NULL & NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL || NULL' should return NULL
SELECT NULL || NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL << NULL' should return NULL
SELECT NULL << NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL >> NULL' should return NULL
SELECT NULL >> NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL + NULL' should return NULL
SELECT NULL + NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL - NULL' should return NULL
SELECT NULL - NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL * NULL' should return NULL
SELECT NULL * NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL / NULL' should return NULL
SELECT NULL / NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NULL % NULL' should return NULL
SELECT NULL % NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: '- NULL' should return NULL
SELECT - NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: '+ NULL' should return NULL
SELECT + NULL as res;

-- expect:
-- | res  |
-- | NULL |

-- name: 'NOT NULL' should return NULL
SELECT NOT NULL as res;

-- expect:
-- | res  |
-- | NULL |
