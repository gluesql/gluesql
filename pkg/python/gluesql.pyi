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
    def __init__(): ...
    def path(path_arg: str) -> "SledStorage": ...
    def temporary(temporary: bool) -> "SledStorage": ...
    def use_compression(use_compression: bool) -> "SledStorage": ...
    def print_profile_on_drop(print_profile_on_drop: bool) -> "SledStorage": ...
    def compression_factor(compression_factor: int) -> "SledStorage": ...
    def create_new(create_new: bool) -> "SledStorage": ...
    def cache_capacity(cache_capacity: int) -> "SledStorage": ...
    def mode(mode: "SledStorageModeConfig") -> "SledStorage": ...
    def flush_every_ms(every_ms: int): ...

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
