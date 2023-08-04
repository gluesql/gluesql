"""
Type hints for Native Rust Extension
"""

from abc import ABCMeta


class Storage(metaclass=ABCMeta):
    pass


class Glue:
    def query(sql: str): ...
    def set_default_engine(engine: Storage): ...


class MemoryStorage(Storage):
    pass


class JsonStorage(Storage):
    def __init__(path: str): ...


class SharedMemoryStorage(Storage):
    def __init__(path: str): ...


class ParsingError(Exception):
    """ """


class TranslateError(Exception):
    """ """


class ExecuteError(Exception):
    """ """


class PlanError(Exception):
    """ """
