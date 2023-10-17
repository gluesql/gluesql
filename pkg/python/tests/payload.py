from gluesql import Glue, MemoryStorage


def test_payload():
    db = Glue(MemoryStorage())

    assert (
        db.query(
            """
    CREATE TABLE Foo (id INTEGER);
    CREATE TABLE Bar;
    """
        )
        == [{"type": "CREATE TABLE"}, {"type": "CREATE TABLE"}]
    )

    assert (
        db.query(
            """
    INSERT INTO Foo VALUES (1), (2), (3)
    """
        )
        == [{"type": "INSERT", "affected": 3}]
    )

    assert (
        db.query(
            """
    INSERT INTO Bar VALUES
        ('{ "hello": 1 }'),
        ('{ "world": "cookie" }');
    """
        )
        == [{"type": "INSERT", "affected": 2}]
    )

    assert (
        db.query(
            """
    SELECT * FROM Bar
    """
        )
        == [{"type": "SELECT", "rows": [{"hello": 1}, {"world": "cookie"}]}]
    )

    assert (
        db.query(
            """
    SELECT * FROM Bar
            """
        )
        == [{"type": "SELECT", "rows": [{"hello": 1}, {"world": "cookie"}]}]
    )

    assert (
        db.query(
            """
    SELECT * FROM Foo
            """
        )
        == [{"type": "SELECT", "rows": [{"id": 1}, {"id": 2}, {"id": 3}]}]
    )

    assert (
        db.query(
            """
    UPDATE Foo SET id = id + 2 WHERE id = 3
            """
        )
        == [{"type": "UPDATE", "affected": 1}]
    )

    assert (
        db.query(
            """
    DELETE FROM Foo WHERE id < 5
            """
        )
        == [{"type": "DELETE", "affected": 2}]
    )

    assert (
        db.query(
            """
    SELECT * FROM Foo
            """
        )
        == [{"type": "SELECT", "rows": [{"id": 5}]}]
    )

    assert (
        db.query(
            """
    SHOW COLUMNS FROM Foo
            """
        )
        == [{"type": "SHOW COLUMNS", "columns": [{"name": "id", "type": "INT"}]}]
    )

    assert (
        db.query(
            """
    SHOW TABLES
            """
        )
        == [{"type": "SHOW TABLES", "tables": ["Bar", "Foo"]}]
    )

    assert (
        db.query(
            """
    DROP TABLE IF EXISTS Foo
            """
        )
        == [{"type": "DROP TABLE"}]
    )
