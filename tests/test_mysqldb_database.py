import pytest
from pytest_mock import MockerFixture

from pyframework import (
    MysqldbDatabase,
    MysqldbTranslator,
)
from pyframework.core import (
    AbstractMySqlDatabase,
    Column,
    Table,
)
import MySQLdb


def test_default_translator(mocker: MockerFixture):
    mocker.patch.object(AbstractMySqlDatabase, "__init__")
    MysqldbDatabase()
    AbstractMySqlDatabase.__init__.assert_has_calls(
        [mocker.call(Translator=MysqldbTranslator)]
    )


def test_database_connect(mocker: MockerFixture):
    db = MysqldbDatabase("loc", "user", "password", "database")
    mocker.patch.object(MySQLdb, "connect")

    query = mocker.MagicMock()
    query.return_value = []
    mocker.patch.object(db, "query", query)
    db.connect()
    MySQLdb.connect.assert_has_calls(
        [
            mocker.call(
                "loc",
                "user",
                "password",
                "database",
                use_unicode=True,
                charset="utf8mb4",
            )
        ]
    )

    query.assert_has_calls([mocker.call("show", "tables")])
    db.close()

    db.query = lambda a, b: [{"": "foo"}] if a == "show" else []
    query_spy = mocker.spy(db, "query")
    mocker.patch.object(db, "query", query_spy)
    db.connect()
    query_spy.assert_has_calls(
        [mocker.call("show", "tables"), mocker.call("describe", "foo")]
    )

    assert "foo" in db.table_names


def test_database_db_query(mocker: MockerFixture):
    db = MysqldbDatabase("loc", "user", "password", "database")
    with pytest.raises(Exception, match="initiate the connection"):
        db.db_query("")

    db._db = mocker.Mock()
    db._db.open = True

    with pytest.raises(TypeError, match="Database.query a str"):
        db.db_query(1)

    with pytest.raises(TypeError, match="maxrows must be an int"):
        db.db_query("", maxrows="")
    with pytest.raises(ValueError, match="maxrows must be positive"):
        db.db_query("", maxrows=-1)

    with pytest.raises(TypeError, match="how must be an int"):
        db.db_query("", how="")
    with pytest.raises(ValueError, match="how must be 0, 1, or 2"):
        db.db_query("", how=4)

    db._db.store_result.return_value = None
    assert not db.db_query("")

    db._db.store_result.return_value = mocker.Mock()
    db._db.store_result.return_value.fetch_row.return_value = [
        {"a": bytes("foo", "utf8")}
    ]
    assert db.db_query("") == [{"a": "foo"}]
    # need to test more complicated conversion from bytes


def test_translator(mocker: MockerFixture):
    mocker.patch.object(MySQLdb, "escape_string")

    db = MysqldbDatabase("loc", "user", "password", "database")
    db.translator.escape_string("foo")
    MySQLdb.escape_string.assert_has_calls([mocker.call("foo")])

    results, expect = None, None
    assert db.translator.interpret(results, method="foo", table="bar") == expect

    results, expect = ("foo", "bar"), ["foo", "bar"]
    assert db.translator.interpret(results, method="select", table="baz") == expect
    assert db.translator.interpret(results, method="distinct", table="baz") == expect

    results, expect = [["foo"]], "foo"
    assert (
        db.translator.interpret(results, method="count", table="bar", how=0) == expect
    )

    results, expect = [{"a": "foo"}], "foo"
    assert db.translator.interpret(results, method="count", table="bar") == expect

    results, expect = [("foo", "bar")], ["`foo` bar"]
    assert (
        db.translator.interpret(results, method="describe", table="baz", how=0)
        == expect
    )

    results, expect = [{"COLUMNS.Field": "foo", "COLUMNS.Type": "bar"}], ["`foo` bar"]
    assert (
        db.translator.interpret(results, method="describe", table="baz", how=2)
        == expect
    )

    results, expect = [{"Field": "foo", "COLUMNS.Type": "bar"}], ["`foo` bar"]
    assert db.translator.interpret(results, method="describe", table="baz") == expect

    results, expect = [("foo",), ("bar",)], ["foo", "bar"]
    assert (
        db.translator.interpret(results, method="show", table="tables", how=0) == expect
    )

    results, expect = [{"foo": "bar"}], ["bar"]
    assert db.translator.interpret(results, method="show", table="tables") == expect
