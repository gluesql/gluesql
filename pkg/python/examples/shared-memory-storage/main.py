from gluesql import Glue, SharedMemoryStorage
from tabulate import tabulate

db = Glue(SharedMemoryStorage())


def run():
    db.query(
        """
        CREATE TABLE User (id INTEGER, name TEXT);
        CREATE TABLE Device (name TEXT, userId INTEGER);
        INSERT INTO User VALUES
        (1, 'glue'), (2, 'sticky'), (3, 'watt');
        INSERT INTO Device VALUES
        ('Phone', 1), ('Mic', 1), ('Monitor', 3),
        ('Mouse', 2), ('Touchpad', 2);
        """
    )

    sql = "SHOW TABLES;"
    result = db.query(sql)
    tables = result[0].get("tables")
    tables = list(map(lambda t: [t], tables))
    print(f"\n[Query]\n{sql}")
    print(
        tabulate(tables, headers=["Values"], showindex=True, tablefmt="simple_outline")
    )

    sql = """
        SELECT
        u.name as user,
        d.name as device
        FROM User u
        JOIN Device d ON u.id = d.userId
    """.strip().replace(
        "    ", ""
    )

    result = db.query(sql)
    rows = result[0].get("rows")
    print(f"\n[Query]\n{sql}")
    print(tabulate(rows, headers="keys", showindex=True, tablefmt="simple_outline"))


if __name__ == "__main__":
    run()
