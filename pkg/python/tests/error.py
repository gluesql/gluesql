import pytest
from gluesql import EngineNotLoadedError, Glue


def test_engine_not_loaded():
    db = Glue()
    sql = ""
    with pytest.raises(EngineNotLoadedError):
        db.query(sql)
