import abc
from typing import Any

from .Table import Column, Table


class AbstractConstraint(abc.ABC):
    @property
    def target_table(self):
        return self._target_table

    @property
    def target_column(self):
        return self._target_column

    @classmethod
    def on_column(cls, column: Column, *args, **kwargs):
        return cls(column.table, column, *args, **kwargs)

    @classmethod
    def _add_to_column(cls, self, table, column):
        if not hasattr(table, "_relations"):
            setattr(table, "_relations", [])
        table._relations.append(self)

        if not hasattr(column, "_relations"):
            setattr(column, "_relations", [])
        column._relations.append(self)


class ForeignKey(AbstractConstraint):
    def __init__(
        self,
        target_table: Table,
        target_column: Column,
        foreign_table: Table,
        foreign_column: Column,
    ):
        self._target_table = target_table
        self._target_column = target_column
        AbstractConstraint._add_to_column(self, self._target_table, self._target_column)

        self._foreign_table = foreign_table
        self._foreign_column = foreign_column
        AbstractConstraint._add_to_column(
            self, self._foreign_table, self._foreign_column
        )

    @property
    def foreign_table(self):
        return self._foreign_table

    @property
    def foreign_column(self):
        return self._foreign_column

    @classmethod
    def column_to_column(cls, target: Column, foreign: Column):
        return cls(target.table, target, foreign.table, foreign)
