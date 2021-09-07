import pytest
from pytest_mock import MockerFixture
from typing import Any

from pyframework.core import Column, ForeignKey, Index, PrimaryKey, Table, Unique


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

    constraint = mocker.Mock()
    constraint.drop = mocker.MagicMock()
    tbl.columns[0]._constraints.append(constraint)
    tbl.drop_key(constraint)
    assert tbl.db.query.called_with(mocker.call("drop constraint", fields=constraint))
    assert constraint.drop.called_once()


def test_constraints(mocker: MockerFixture):
    col = Column.from_definition("name dtype not null")
    col2 = Column.from_definition("test dtype unique")

    table = mocker.Mock()
    table.columns = [col, col2]
    table.select = mocker.MagicMock()
    table.select.return_value = [
        {"name": "foo", "test": "bar"},
        {"name": "biz", "test": "buz"},
    ]
    table.distinct = table.select

    index = Index(col, name="foobar")
    mocker.patch.object(index.target, "_table", table)
    col = index.target

    spy = mocker.spy(index, "prepare")
    assert index.validate(None) == index
    spy.assert_called_once()
    assert table.select.called_with(mocker.call(["name", "test"]))
    assert hasattr(index, "values")
    assert index.values == {"foo": {"test": "bar"}, "biz": {"test": "buz"}}

    table.select.reset_mock()
    table.select.return_value = [{"name": "foo"}, {"name": "biz"}]

    unique = Unique(col, name="foobar2")
    spy = mocker.spy(unique, "prepare")
    assert unique.validate(None) == unique
    spy.assert_called_once()
    assert table.select.called_with(mocker.call("name"))
    assert hasattr(unique, "values")
    assert unique.values == ["foo", "biz"]
    with pytest.raises(ValueError, match="is not unique"):
        unique.validate("foo")

    with pytest.raises(ValueError, match="may only be created"):
        PrimaryKey(col2)

    table.select.reset_mock()
    table.select.return_value = [
        {"name": "foo", "test": "bar"},
        {"name": "biz", "test": "buz"},
    ]
    col2._table = table
    col._table = None

    col2._constraints = []
    with pytest.raises(ValueError, match="table on itself"):
        ForeignKey(col, col)
    fk = ForeignKey(col, col2, "foobar3")
    spy = mocker.spy(fk, "prepare")
    assert col2.get_constraint("index") is not None

    assert fk.validate("bar")
    spy.assert_called_once()

    fk.drop()


def test_get_constraint():
    col = Column.from_definition("name dtype not null")
    col._constraints = []

    assert col.get_constraint("index") is None
    assert col.get_constraint("unique") is None
    assert col.get_constraint("primary") is None

    index = Index(col)
    assert index in col.constraints
    assert col.get_constraint("index") == index

    unique = Unique(col)
    assert index in col.constraints and unique in col.constraints
    assert col.get_constraint("unique") == unique
    assert col.get_constraint("index") == unique
    assert index not in col.constraints

    index = Index(col)
    primary = PrimaryKey(col)
    assert all([i in col.constraints for i in [unique, index, primary]])
    assert col.get_constraint("primary") == primary
    assert col.get_constraint("unique") == primary
    assert unique not in col.constraints
    assert col.get_constraint("index") == primary
    assert index not in col.constraints

    col2 = Column.from_definition("name2 dtype unique", table="Table2")
    fk = ForeignKey(col, col2)
    assert fk in col.constraints and primary in col.constraints
    assert col.get_constraint("fk_None_name_name2") == fk
    assert col.get_constraint(fk) == fk
