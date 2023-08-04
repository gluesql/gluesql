from gluesql import Glue, MemoryStorage


def test_memstore_create_table():
    db = Glue()
    db.set_default_engine(MemoryStorage())

    sql = """
        CREATE TABLE Mem (mid INTEGER) ENGINE = memory;
        CREATE TABLE Loc (lid INTEGER) ENGINE = localStorage;
        CREATE TABLE Ses (sid INTEGER) ENGINE = sessionStorage;
        CREATE TABLE Idb (iid INTEGER) ENGINE = indexedDB;
    """

    expected = [
        {"type": "CREATE TABLE"},
        {"type": "CREATE TABLE"},
        {"type": "CREATE TABLE"},
        {"type": "CREATE TABLE"},
    ]

    assert db.query(sql) == expected
