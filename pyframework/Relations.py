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
    def _add_to_column(cls, instance, table, column):
        if not hasattr(table, "_relations"):
            setattr(table, "_relations", [])
        table._relations.append(instance)

        if not hasattr(column, "_relations"):
            setattr(column, "_relations", [])
        column._relations.append(instance)


class ForeignKey(AbstractConstraint):
    def __init__(
        self,
        target: Union[Tuple[Table, Column], Column],
        foreign: Union[Tuple[Table, Column], Column],
    ):
        if isinstance(target, Iterable) and len(target) == 2:
            self._target_table, self._target_column = target
        elif isinstance(target, Column):
            self._target_table, self._target_column = target.table, target
        AbstractConstraint._add_to_column(self, self._target_table, self._target_column)

        if isinstance(foreign, Iterable) and len(foreign) == 2:
            self._foreign_table, self._foreign_column = foreign
        elif isinstance(foreign, Column):
            self._foreign_table, self._foreign_column = foreign.table, foreign
        AbstractConstraint._add_to_column(
            self, self._foreign_table, self._foreign_column
        )

    @property
    def foreign_table(self):
        return self._foreign_table

    @property
    def foreign_column(self):
        return self._foreign_column
