"""
Type hints for Native Rust Extension
"""

from abc import ABCMeta
from typing import Any, Final

class Glue:
    def __init__(self, storage: "Storage") -> None: ...
    def query(sql: str): ...

class Storage(metaclass=ABCMeta):
    pass

class MemoryStorage(Storage):
    def __init__(): ...

class SharedMemoryStorage(Storage):
    def __init__(): ...

class JsonStorage(Storage):
    def __init__(path: str): ...

class SledStorageModeConfig:
    """
    In this mode, the database will make
    decisions that favor using less space
    instead of supporting the highest possible
    write throughput. This mode will also
    rewrite data more frequently as it
    strives to reduce fragmentation.
    """
    LowSpace: Final[Any]

    """
    In this mode, the database will try
    to maximize write throughput while
    potentially using more disk space.
    """
    HighThroughput: Final[Any]

class SledStorageConfig:
    def __init__(
        *,
        path: str,
        temporary: bool,
        use_compression: bool,
        print_profile_on_drop: bool,
        compression_factor: int,
        create_new: bool,
        cache_capacity: int,
        mode: "SledStorageModeConfig",
    ): ...
    path: str
    temporary: bool
    use_compression: bool
    print_profile_on_drop: bool
    compression_factor: int
    create_new: bool
    cache_capacity: int
    mode: "SledStorageModeConfig"

class SledStorage(Storage):
    def __init__(path: str): ...
    @staticmethod
    def try_from(config: "SledStorageConfig") -> "SledStorage": ...

class GlueSQLError(Exception):
    pass
