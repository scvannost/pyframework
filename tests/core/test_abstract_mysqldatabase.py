import pytest
from pytest_mock import MockerFixture
from typing import Any

from pyframework.core import (
    AbstractMySqlDatabase,
    AbstractMySqlTranslator,
    Column,
    Index,
    Table,
)


class MockMySqlDB(AbstractMySqlDatabase):
    def __init__(self, *args, **kwargs):
        class MockMySqlTranslator(AbstractMySqlTranslator):
            def escape_string(self, string: str) -> str:
                return string

            def interpret(self, results: Any, *args, **kwargs) -> Any:
                return results

        super().__init__(*args, **kwargs, Translator=MockMySqlTranslator)

    def connect(self):
        pass

    def db_query(self, q: str, limit: int = -1, **kwargs):
        return q

    @property
    def dtypes(self):
        return ["int", "float", "double", "char", "text"]

    @property
    def NULL(self):
        return "NULL"


@pytest.fixture
def mockdb(mocker: MockerFixture) -> MockMySqlDB:
    db = MockMySqlDB("loc", "user", "password", "database")
    mocker.patch.object(db, "_db")
    db._db.open = True

    mocker.patch.object(
        db,
        "_tables",
        [
            Table(db, "foo", []),
            Table(db, "bar", []),
        ],
    )

    db.foo = db._tables[0]
    db.bar = db._tables[1]

    for c in db.foo.columns:
        c._table = db.foo
    for c in db.bar.columns:
        c._table = db.bar

    return db


def test_properties(mockdb: MockMySqlDB):
    # connect and table_names both have functionality
    assert not MockMySqlDB("", "", "", "").open
    # because self.db is None

    assert mockdb.open

    assert 2 == len(mockdb.table_names)
    assert "foo" in mockdb.table_names
    assert "bar" in mockdb.table_names


def test_simple_functions(mockdb: MockMySqlDB):
    mockdb.commit()
    assert mockdb.db.commit.called_once()

    mockdb.rollback()
    assert mockdb.db.rollback.called_once()

    db = mockdb.db
    mockdb.close()
    assert db.close.called_once()


def test_reconnect(mockdb: MockMySqlDB, mocker: MockerFixture):
    db = mockdb.db
    spy = mocker.spy(db, "connect")

    mockdb.reconnect()
    assert db.close.called_once()
    assert spy.called_once()


def test_helper_functions(mockdb: MockMySqlDB, mocker: MockerFixture):
    assert mockdb.get_table("foo") is mockdb.foo
    assert mockdb.get_table("bar") is mockdb.bar

    assert mockdb.get_table(mockdb.tables[0]) is mockdb.foo
    assert mockdb.get_table(mockdb.tables[1]) is mockdb.bar

    assert mockdb.get_table("test") is None

    foo_get_column = mocker.patch.object(mockdb.foo, "get_column")
    bar_get_column = mocker.patch.object(mockdb.bar, "get_column")

    assert mockdb.get_column("foo", "index")
    assert foo_get_column.called_once()
    foo_get_column.reset()

    mockdb.get_column("bar", Column("", "", ""))
    assert bar_get_column.called_once()

    assert mockdb.get_column("test", "column") is None

    assert not mockdb.is_valid_column("test", "index int")
    assert not mockdb.is_valid_column("foo", "index")
    assert foo_get_column.not_called()

    assert not mockdb.is_valid_column("foo", "index baz")
    assert foo_get_column.called_once()
    foo_get_column.reset()

    foo_get_column.return_value = None
    assert mockdb.is_valid_column("foo", "index int")
    assert foo_get_column.called_once()
    foo_get_column.reset()

    foo_get_column.return_value = "index"
    assert not mockdb.is_valid_column("foo", "index int")
    assert foo_get_column.called_once()
    foo_get_column.reset()

    assert not mockdb.is_valid_column("foo", Column("", "", ""))
    assert foo_get_column.not_called()

    foo_get_column.return_value = None
    assert mockdb.is_valid_column("foo", Column("", "", dtype="int"))
    assert foo_get_column.called_once()
    foo_get_column.reset()

    foo_get_column.return_value = "index"
    assert not mockdb.is_valid_column("foo", Column("", "", dtype="int"))
    assert foo_get_column.called_once()
    foo_get_column.reset()

    assert not mockdb.is_valid_column("foo", 4)
    assert foo_get_column.not_called()

    foo_get_column.return_value = None
    assert mockdb.is_valid_column("foo", "index test int")
    assert foo_get_column.called_with("index test")


def test_passing_to_query(mockdb: MockMySqlDB, mocker: MockerFixture):
    def mock_query(method, table, *args, **kwargs):
        if method == "make table":
            return Table(None, "table", [])
        elif method == "describe":
            return ["field1 int, field2 int"]

    mockdb.query = mock_query
    query = mocker.spy(mockdb, "query")

    mockdb.make_table("table", ["field1 int", "field2 int"])
    query.assert_has_calls(
        [
            mocker.call(
                "create table",
                "table",
                ["field1 int", "field2 int"],
                temporary=False,
                clobber=False,
            ),
            mocker.call("describe", "table"),
        ]
    )
    tbl = mockdb.tables[-1]
    assert tbl.name == "table"
    assert tbl.db == mockdb
    assert tbl.column_names == ["field1", "field2"]
    query.reset_mock()

    tbl = mockdb.move_table("bar", "new")  # throwing
    query.assert_has_calls(
        [mocker.call("rename", "bar", "new"), mocker.call("describe", "new")]
    )
    tbl = mockdb.tables[-1]
    assert tbl.name == "new"
    assert tbl.db == mockdb
    assert tbl.column_names == ["field1", "field2"]
    query.reset_mock()

    mockdb.truncate_table("table")
    query.assert_called_once_with("truncate", "table")
    query.reset_mock()

    mockdb.add_column("table", "field", after="first")
    query.assert_has_calls(
        [
            mocker.call("add column", "table", "field", after="first"),
            mocker.call("describe", "table"),
        ]
    )
    query.reset_mock()

    mockdb.drop_column("table", "field")
    query.assert_has_calls(
        [mocker.call("drop column", "table", "field"), mocker.call("describe", "table")]
    )
    query.reset_mock()

    mockdb.alter_column("table", "old", "new")
    query.assert_has_calls(
        [
            mocker.call("alter column", "table", "old", to="new"),
            mocker.call("describe", "table"),
        ]
    )
    query.reset_mock()

    mockdb.drop_table("table")
    query.assert_called_once_with("drop table", "table", temporary=False)
    query.reset_mock()


def test_sql_generators(mockdb: MockMySqlDB):
    assert mockdb.translator.groupby("foo") == " group by 'foo'"
    assert mockdb.translator.limit(2) == " limit 2"
    assert mockdb.translator.orderby("foo") == " order by 'foo'"
    assert mockdb.translator.where("foo") == " where 'foo'"

    assert (
        mockdb.translator.join(mockdb.foo, mockdb.bar, "baz", "inner")
        == "foo inner join bar on 'baz'"
    )
    assert (
        mockdb.translator.join(mockdb.foo, mockdb.bar, "baz", "inner", alias="table")
        == "foo inner join bar as 'table' on 'baz'"
    )


def test_operations(mockdb: MockMySqlDB):
    assert str(mockdb.add("a", "b")) == "('a' + 'b') operation"
    assert str(mockdb.div("a", "b")) == "('a' / 'b') operation"
    assert str(mockdb.mul("a", "b")) == "('a' * 'b') operation"
    assert str(mockdb.sub("a", "b")) == "('a' - 'b') operation"


def test_comparisons(mockdb: MockMySqlDB):
    assert str(mockdb.contains("a", "b")) == "('a' in 'b') comparison"
    assert str(mockdb.eq("a", "b")) == "('a' = 'b') comparison"
    assert str(mockdb.ge("a", "b")) == "('a' >= 'b') comparison"
    assert str(mockdb.gt("a", "b")) == "('a' > 'b') comparison"
    assert str(mockdb.le("a", "b")) == "('a' <= 'b') comparison"
    assert str(mockdb.like("a", "b")) == "('a' like 'b') comparison"
    assert str(mockdb.logical_and("a", "b")) == "('a' and 'b') comparison"
    assert str(mockdb.logical_or("a", "b")) == "('a' or 'b') comparison"
    assert str(mockdb.lt("a", "b")) == "('a' < 'b') comparison"
    assert str(mockdb.ne("a", "b")) == "('a' <> 'b') comparison"


def test_query_validation(mockdb: MockMySqlDB, mocker: MockerFixture):
    with pytest.raises(TypeError, match=r"str"):
        mockdb.query(3, "table")

    with pytest.raises(TypeError, match=r"Table or str"):
        mockdb.query("method", 2)

    with pytest.raises(ValueError, match=r"cannot be a table in self.tables"):
        mockdb.query("create table", "foo")

    with pytest.raises(ValueError, match=r"must be a table in self.tables"):
        mockdb.query("method", "table")

    with pytest.raises(TypeError, match=r"Mapping"):
        mockdb.query("insert", "foo", "not a dict")

    with pytest.raises(TypeError, match=r"Mapping\[col, value\]"):
        mockdb.query("insert", "foo", {3: "test"})

    with pytest.raises(ValueError, match=r"keys in @table.columns"):
        mockdb.query("insert", "foo", {"field": 3})

    with pytest.raises(TypeError, match=r"Mapping"):
        mockdb.query("update", "foo", "not a dict")

    with pytest.raises(TypeError, match=r"Mapping\[col, value\]"):
        mockdb.query("update", "foo", {3: "test"})

    with pytest.raises(ValueError, match=r"keys in @table.columns"):
        mockdb.query("update", "foo", {"field": 3})

    with pytest.raises(TypeError, match=r"must be a str"):
        mockdb.query("rename table", "foo", 3)

    with pytest.raises(ValueError, match=r"must not be an existing table"):
        mockdb.query("rename table", "foo", "bar")

    with pytest.raises(TypeError, match=r"must be a column in @table"):
        mockdb.query("drop column", "foo", 3)

    with pytest.raises(ValueError, match=r"must be a column in @table"):
        mockdb.query("drop column", "foo", "bar")

    with pytest.raises(TypeError, match=r"must be a str or Iterable\(col\)"):
        mockdb.query("create table", "table", 3)

    with pytest.raises(ValueError, match=r"must be a str or Iterable\(col\)"):
        mockdb.query("create table", "table", "index")

    with pytest.raises(ValueError, match=r"may not contain duplicate column names"):
        mockdb.query("create table", "table", ["index int", "index float"])

    with pytest.raises(ValueError, match=r"may not contain duplicate column names"):
        mockdb.query(
            "create table",
            "table",
            ["index int", Column("", name="index", dtype="float")],
        )

    with pytest.raises(ValueError, match=r"must be a valid column definition"):
        mockdb.query("add column", "foo", 3)

    with pytest.raises(ValueError, match=r"must be a column in the table"):
        mockdb.query("add column", "foo", "index int", after=3)

    with pytest.raises(ValueError, match=r"only a single key"):
        mockdb.query("alter column", "foo", {"a": 1, "b": 2})

    with pytest.raises(ValueError, match=r"@to should not be specified"):
        mockdb.query("alter column", "foo", {"a": 1}, to="index")

    with pytest.raises(ValueError, match=r"must be a column in @table"):
        mockdb.query("alter column", "foo", "bar")

    mocker.patch.object(mockdb.foo, "_columns", [Table("", "bar", [])])
    with pytest.raises(ValueError, match=r"@to must be provided"):
        mockdb.query("alter column", "foo", "bar")

    with pytest.raises(ValueError, match=r"@to must be a valid column definition for"):
        mockdb.query("alter column", "foo", "bar", to="index")

    with pytest.raises(TypeError, match=r"must be 'all' or Iterable\(col\)"):
        mockdb.query("select", "foo", [3])

    mocker.patch.object(mockdb.foo, "_columns", [])
    with pytest.raises(ValueError, match=r"must be 'all' or Iterable\(col\)"):
        mockdb.query("select", "foo", "3")

    with pytest.raises(ValueError, match=r"allowed values of Database.query@method"):
        mockdb.query("method", "foo")

    mocker.patch.object(mockdb.foo, "_columns", [Table("", "bar", [])])
    with pytest.raises(
        TypeError, match=r"@where must be a Column with dtype \"comparison\""
    ):
        mockdb.query("select", "foo", "bar", where="baz")

    with pytest.raises(TypeError, match=r"@limit must be a positive int"):
        mockdb.query("select", "foo", "bar", limit="baz")

    with pytest.raises(ValueError, match=r"@limit must be a positive int"):
        mockdb.query("select", "foo", "bar", limit=-1)

    with pytest.raises(TypeError, match=r"@groupby must be a column in @table"):
        mockdb.query("select", "foo", "bar", groupby=3)

    with pytest.raises(ValueError, match=r"@groupby must be a column in @table"):
        mockdb.query("select", "foo", "bar", groupby="baz")

    with pytest.raises(TypeError, match=r"@orderby must be a column in @table"):
        mockdb.query("select", "foo", "bar", orderby=3)

    with pytest.raises(ValueError, match=r"@orderby must be a column in @table"):
        mockdb.query("select", "foo", "bar", orderby="baz")

    mocker.patch.object(mockdb.foo, "_columns", [Column(mockdb.foo, "bar", "dtype")])
    with pytest.raises(ValueError, match=r"must not contain multiple constraints"):
        mockdb.query("add foreign index", "foo", "bar")

    with pytest.raises(ValueError, match=r"must be a column in @table"):
        mockdb.query("add foreign", "foo", 3)

    with pytest.raises(ValueError, match="Cannot foreign key a table"):
        mockdb.query(
            "add foreign", "foo", "bar", foreign=Column(mockdb.foo, "foreign", "dtype")
        )

    with pytest.raises(TypeError, match="@name must be a str"):
        mockdb.query("add index", "foo", "bar", name=3)

    with pytest.raises(ValueError, match="@name cannot be over 64 characters"):
        mockdb.query("add index", "foo", "bar", name="12345678" * 9)

    with pytest.raises(TypeError, match="@fields must be an AbstractConstraint"):
        mockdb.query("drop constraint", "foo", 3)


def test_query_translate(mockdb: MockMySqlDB, mocker: MockerFixture):
    tbl = Table(None, "table", [])
    tbl._columns.append(Column(tbl, "column", "int"))

    assert (
        mockdb.query(
            "create table",
            tbl,
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "create table 'table' if not exists ('column' int);"
    )
    mockdb._tables.append(tbl)
    tbl._db = mockdb
    assert (
        mockdb.query(
            "drop table",
            tbl,
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "drop table 'table' if exists;"
    )
    mockdb._tables = []

    assert (
        mockdb.query("create table", "table", "column int")
        == "create table 'table' if not exists (column int);"
    )
    mockdb._tables.append(tbl)
    assert mockdb.query("drop table", "table") == "drop table 'table' if exists;"
    mockdb._tables = []

    tbl._columns.append(Column(tbl, "new_column", "text"))
    assert (
        mockdb.query("create table", "table", "column int, new_column text")
        == "create table 'table' if not exists (column int, new_column text);"
    )
    assert (
        mockdb.query("create table", "table", ["column int", "new_column text"])
        == "create table 'table' if not exists (column int, new_column text);"
    )
    mockdb._tables.append(tbl)
    assert (
        mockdb.query(
            "show",
            "tables",
            "foobar",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "show tables;"
    )
    assert (
        mockdb.query(
            "describe",
            "table",
            "foobar",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "describe 'table';"
    )

    assert (
        mockdb.query(
            "insert",
            "table",
            {"column": "value1", "new_column": "value2"},
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "insert into 'table' ('column', 'new_column') values (value1, value2);"
    )
    assert (
        mockdb.query(
            "update",
            "table",
            {"column": "new_value"},
            groupby="all",
            orderby="these",
            limit="ignored",
        )
        == "update 'table' set 'column' = new_value;"
    )

    assert (
        mockdb.query(
            "rename table",
            "table",
            "new_name",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "alter table 'table' rename 'new_name';"
    )
    mockdb._tables[0]._name = "new_name"
    assert (
        mockdb.query("rename table", "new_name", to="table")
        == "alter table 'new_name' rename 'table';"
    )
    mockdb._tables[0]._name = "table"

    mockdb.tables[0]._columns = []
    assert (
        mockdb.query(
            "add column",
            "table",
            "column int",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "alter table 'table' add column column int;"
    )
    mockdb.tables[0]._columns.append(Column(tbl, "column", "int"))
    col = Column(mockdb.get_table("table"), "new_column", "blob")
    assert (
        mockdb.query("add column", "table", col)
        == "alter table 'table' add column 'new_column' blob;"
    )
    mockdb.tables[0]._columns.append(col)

    column, new_column = mockdb.tables[0]._columns
    assert (
        mockdb.query("select", "table", "all", groupby=column)
        == "select * from 'table' group by table.column;"
    )
    assert (
        mockdb.query("select", "table", "all", orderby=new_column)
        == "select * from 'table' order by table.new_column;"
    )
    assert (
        mockdb.query("select", "table", "all", where=(column < 0))
        == "select * from 'table' where (table.column < 0);"
    )
    assert (
        mockdb.query("select", "table", "all", limit=3)
        == "select * from 'table' limit 3;"
    )

    tbl2 = Table(mockdb, "new_table", [])
    tbl2._columns.append(Column(tbl2, "column", "int"))

    mockdb._tables.append(tbl2)
    tbl2 = mockdb.tables[-1]
    column2 = tbl2.columns[0]
    assert (
        mockdb.query("select", tbl.join(tbl2, on=(column == column2)), "all")
        == "select * from table inner join new_table on (table.column = new_table.column);"
    )

    col = Column(mockdb.get_table("table"), "new_altered", "date")
    assert (
        mockdb.query(
            "alter column",
            "table",
            "column",
            to="altered text",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "alter table 'table' change column 'column' altered text;"
    )
    assert (
        mockdb.query("alter column", "table", "new_column", to=col)
        == "alter table 'table' change column 'new_column' 'new_altered' date;"
    )

    assert (
        mockdb.query(
            "add index",
            "table",
            "column",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "alter table 'table' add constraint index (column);"
    )

    assert (
        mockdb.query(
            "add foreign",
            "table",
            "column",
            foreign=mockdb.get_column("new_table", "column"),
            name="fk_foobar",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "alter table 'table' add constraint fk_foobar foreign key (column) references new_table (column);"
    )

    assert (
        mockdb.query(
            "drop constraint",
            "table",
            Index(mockdb.get_column("new_table", "column"), name="foo"),
        )
        == "alter table 'table' drop constraint foo;"
    )

    assert (
        mockdb.query(
            "truncate",
            "table",
            "foobar",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "truncate table 'table';"
    )
    assert (
        mockdb.query(
            "delete",
            "table",
            "foobar",
            where=(column < 0),
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "delete from 'table' where (table.column < 0);"
    )

    assert (
        mockdb.query(
            "drop table",
            "table",
            "foobar",
            where="all",
            groupby="of",
            orderby="these",
            limit="ignored",
        )
        == "drop table 'table' if exists;"
    )
    mockdb._tables = []

    assert (
        mockdb.query("add temporary table", "table", "definition int")
        == "create temporary table 'table' if not exists (definition int);"
    )
    mockdb._tables.append(tbl)
    assert (
        mockdb.query("drop table temporary", "table")
        == "drop temporary table 'table' if exists;"
    )
    mockdb._tables = []

    assert (
        mockdb.query("add temporary table if not exists", "table", "definition int")
        == "create temporary table 'table' if not exists (definition int);"
    )
    mockdb._tables.append(tbl)
    assert (
        mockdb.query("drop table", "table", temporary=True)
        == "drop temporary table 'table' if exists;"
    )
    mockdb._tables = []
