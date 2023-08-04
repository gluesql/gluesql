"""
Type hints for Native Rust Extension
"""


class Glue:
    def query(sql: str): ...


class ParsingError(Exception):
    """ """


class TranslateError(Exception):
    """ """


class ExecuteError(Exception):
    """ """


class PlanError(Exception):
    """ """
