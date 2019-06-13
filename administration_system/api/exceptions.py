import psycopg2
from abc import ABC


class DatabaseException(psycopg2.InternalError, ABC):
    pass


class InvalidMember(DatabaseException):
    pass


class InvalidRowCount(DatabaseException):
    pass
