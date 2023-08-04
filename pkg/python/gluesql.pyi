"""
Type hints for Native Rust Extension
"""

from abc import ABCMeta

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

class SledStorage(Storage):
    def __init__(path: str): ...

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
