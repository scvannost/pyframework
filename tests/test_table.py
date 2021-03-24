import pytest
from pytest_mock import MockerFixture
from typing import Any

from pyframework import Column, Table


def test_from_definition():
    col = Column.from_definition("name dtype")
    assert col.name == "name"
    assert col.dtype == "dtype"

    col = Column.from_definition("name dtype not null")
    assert not col.null

    col = Column.from_definition("name dtype null default 'foo bar baz' key")
    assert col.null
    assert col.default == "'foo bar baz'"
    assert col.key

    col = Column.from_definition("name dtype invisible increment unique")
    assert not col.visible
    assert col.increment
    assert col.unique

    col = Column.from_definition("name dtype visible unique primary comment 'foo bar'")
    assert col.visible
    assert col.unique
    assert col.primary
    assert col.comment == "'foo bar'"

    # no checking on dtype, but takes everything up to the next option
    col = Column.from_definition("name foo bar baz")
    assert col.name == "name"
    assert col.dtype == "foo bar baz"


def test_dunder(mocker: MockerFixture):
    assert repr(Column.from_definition("name dtype")) == "name"
    assert (
        repr(Column.from_definition("name dtype", table=Table(None, "table", [])))
        == "table.name"
    )
    assert repr(Column.from_definition("name dtype", table=None)) == "name"

    db = mocker.Mock()
    db.add = mocker.MagicMock()
    db.div = mocker.MagicMock()
    db.mul = mocker.MagicMock()
    db.sub = mocker.MagicMock()

    tbl = Table(db, "table", [])
    col1 = Column(tbl, "name1", "dtype")
    col2 = Column(tbl, "name2", "dtype")

    col1 + col2
    assert db.add.called_once()
    col1 / col2
    assert db.div.called_once()
    col1 * col2
    assert db.mul.called_once()
    col1 - col2
    assert db.sub.called_once()


def test_table(mocker: MockerFixture):
    tbl = Table(mocker.Mock(), "table", [Column(None, "column1", "dtype")])
    col = tbl.columns[0]

    assert tbl.get_column(col) == col
    assert tbl.get_column("column1") == col
    assert tbl.get_column("table.column1") == col
    assert tbl.get_column("foo") is None

    tbl.count()
    assert tbl.db.query.called_with(
        mocker.call(
            "count",
            "table",
            where=None,
            groupby=None,
        )
    )

    tbl.delete()
    assert tbl.db.query.called_with(
        mocker.call(
            "delete",
            "table",
            where=None,
        )
    )

    tbl.distinct()
    assert tbl.db.query.called_with(
        mocker.call(
            "distinct",
            "table",
            fields=None,
            where=None,
            limit=None,
            orderby=None,
        )
    )

    tbl.insert({})
    assert tbl.db.query.called_with(
        mocker.call(
            "insert",
            "table",
            fields={},
        )
    )

    tbl2 = Table(tbl.db, "foo", [])
    tbl.db.join.return_value = "bar"
    join = tbl.join(tbl2, on=Column(tbl, "name", "comparison"))
    assert tbl.db.join(
        mocker.call(
            tbl,
            tbl2,
            on="foo",
            direction="inner",
            alias=None,
        )
    )
    assert join.columns == tbl.columns
    assert join.name == "bar"

    tbl.select()
    assert tbl.db.query.called_with(
        mocker.call(
            "select",
            "table",
            fields=None,
            where=None,
            limit=None,
            groupby=None,
            orderby=None,
        )
    )

    tbl.update()
    assert tbl.db.query.called_with(
        mocker.call(
            "update",
            "table",
            fields=None,
            where=None,
        )
    )
