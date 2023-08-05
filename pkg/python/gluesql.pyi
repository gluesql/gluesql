"""
Type hints for Native Rust Extension
"""

from abc import ABCMeta
from typing import Any, Final

class Glue:
    def query(sql: str): ...
    def set_default_engine(engine: "Storage"): ...

class Storage(metaclass=ABCMeta):
    pass

class MemoryStorage(Storage):
    def __init__(): ...

class SharedMemoryStorage(Storage):
    def __init__(): ...

class JsonStorage(Storage):
    def __init__(path: str): ...

class SledStorageModeConfig:
    LowSpace: Final[Any]
    HighThroughput: Final[Any]

class SledStorageConfig:
    def __init__(
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
    def try_from(config: SledStorageConfig) -> "SledStorage": ...

class GlueSQLError(Exception):
    """ """

class EngineNotLoadedError(GlueSQLError):
    """ """

class ParsingError(GlueSQLError):
    """ """

class TranslateError(GlueSQLError):
    """ """

class ExecuteError(GlueSQLError):
    """ """

class PlanError(GlueSQLError):
    """ """
