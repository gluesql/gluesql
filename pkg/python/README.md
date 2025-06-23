# GlueSQL.py

GlueSQL.py is a Python binding for the [GlueSQL](https://github.com/gluesql/gluesql) database engine. It provides an embedded SQL database that works with a selection of storage backends.

Supported storages:

- `MemoryStorage`
- `JsonStorage`
- `SharedMemoryStorage`
- `SledStorage`

Learn more at **<https://gluesql.org/docs>**.

## Installation

Install from PyPI:

```bash
pip install gluesql
```

## Usage

```python
from gluesql import Glue, MemoryStorage

storage = MemoryStorage()

engine = Glue(storage)

engine.query(
    """
    CREATE TABLE User (id INTEGER, name TEXT);
    INSERT INTO User VALUES (1, 'Hello'), (2, 'World');
    """
)

result = engine.query("SELECT * FROM User;")
print(result)
```

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](https://raw.githubusercontent.com/gluesql/gluesql/main/LICENSE) file for details.
