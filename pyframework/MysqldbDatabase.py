from typing import Any, Dict, Iterable, List, Mapping, Tuple, Type, Union

from .AbstractDatabase import AbstractTranslator
from .AbstractMySqlDatabase import AbstractMySqlDatabase, AbstractMySqlTranslator
from .Table import Column, Table

import MySQLdb


class MysqldbDatabase(AbstractMySqlDatabase):
    """
    An object that represents a MySQL database and allows for interaction with it via MySQLdb


    Parameters
    ----------
    location : str
            the location of the server
    user : str
            the username
    password : str
            the password for @user
    name : str
            the name of the database to use
    Translator : Type[AbstractTranslator] = MysqldbTranslator
            the Translator type for this database
    **kwargs
            passed to super().__init__()
            ignored gracefully

    Properties
    ----------
    db : Any
            the actual database object
    open : bool
            whether or not the database connection is open
    tables : List[Table]
            a Table object for each table in the database
    table_names List[str]
            an list of the names of all the tables
    translator : AbstractTranslator
            a Translator for the particular syntax of the database object

    Methods
    -------
    close()
            closes the connection to the database
    connect()
            connects to the database
    commit()
            commits changes to the database
    db_query(q, limit, kwargs)
            queries the database for q
            additional kwargs ['maxrows', 'how'] passed to self.db.fetch_row
    get_column()
            gets the given column gracefully
    get_table()
            gets the given table gracefully
    interpret()
            takes the response from self.db_query and turns it into the desired python types
    is_valid_column()
            checks if column would be valid
    prepare()
            route to self.translator.prepare
    query(method, table, fields, extra, kwargs)
            provides API for querying the database using self.prepare and self.translate and self.interpret
    rollback()
            rolls back changes to the database
    reconnect()
            same as calling close() then open()
    translate()
            route to self.translator.translate
    validate()
            route to self.translator.validate
    validate_and_raise()
            route to self.translator.validate_and_raise

    Additional Methods
    ------------------
    add_column()
            add the given column to the given table
    add_fk()
            creates a foreign key between table(field) and ref
    alter_column()
            alter the given column in the given table
    drop_column()
            drop the given column from the given table
    drop_fk()
            drops the named foreign key on the given table
    drop_table()
            drop the given table; temporary to only drop TEMPORARY tables
    make_table()
            makes a new table with the given name and fields; clobber to overwrite existing table
    move_table()
            renames the given table to the given name
    truncate_table()
            truncates the given table

    Passthrough Methods
    -------------------
    add(), div(), mul(), sub()
            route to self.translator.[func]()
    contains(), eq(), ge(), gt(), like(), logical_and(), logical_or(), lt(), le(), ne()
            route to self.translator.[func]()
    """

    def __init__(
        self, *args, Translator: Type[AbstractTranslator] = None, **kwargs
    ) -> None:
        if Translator is None:
            Translator = MysqldbTranslator
        super().__init__(*args, Translator=Translator ** kwargs)

    def connect(self) -> None:
        """
        Opens a new connection to the db
        Sets up the self._tables as Iterable[Table]
        If a connection is already open, does nothing
        """
        if self.open:
            return self

        # initiate the connection
        self._db = MySQLdb.connect(
            self._location,
            self._user,
            self._password,
            self._database,
            use_unicode=True,
            charset="utf8mb4",
        )

        # set up the tables
        tables = self.query("show", "tables")
        if tables:
            tables = [list(i.values())[0] for i in tables]
            for i in tables:
                columns = self.query("describe", i)
                setattr(
                    self, i, Table(self, i, [Column.from_defintion(c) for c in columns])
                )

            self._tables = [getattr(self, i) for i in tables]

        return self

    def db_query(
        self, q: str, **kwargs
    ) -> Union[Dict[str, Any], List[Tuple[Any]], List[Dict[str, Any]]]:
        """
        Returns the results of the query as a list of dictionaries unless @how is specified
        If no result, such as UPDATE, DELETE, etc, returns None
        Can raise a MySQLdb.MySQLError if a exception occurs

        Parameters
        ----------
        q : str
                the MySQL query, ending in ';'
                no validation is done for correctness

        **kwargs
                passed to fetch_row() of self.db
                See MySQLdb documentation, eg:
                        maxrows : int = 0
                                the number of rows to return, must be >= 0
                                use 0 for all
                        how : int = 1
                                the format in which to return the results, in [0, 1, 2]
                                1 (default) : tuple of dicts with minimal unique keys
                                0 : tuple of tuples
                                2 : tuple of dicts with fully qualified keys
        """

        # checks
        if not self.open:
            raise Exception("You must initiate the connection.")
        if not type(q) is str:
            raise TypeError("You must pass Database.query a str.")

        # deal with maxrows
        maxrows = kwargs.pop("maxrows", 0)
        if not isinstance(maxrows, int):
            raise TypeError("Database.db_query@maxrows must be an int.")
        elif maxrows < 0:
            raise ValueError("Database.db_query@maxrows must be positibe")

        # deal with `how` kwarg
        how = kwargs.pop("how", 1)  # default to dictionary
        if not isinstance(how, int):
            "Database.db_query@how must be an int."
        elif not 0 <= how <= 2:
            "Database.db_query@how must be 0, 1, or 2."

        # run the call
        self._db.query(q)
        r = self._db.store_result()

        # return None if no results
        if r is None:
            return r

        # get the results
        ret = list(r.fetch_row(maxrows=maxrows, how=how))

        # handle MySQL `bytes` type and convert to str or int
        if ret and len(ret) > 0 and type(ret[0]) is dict:  # if is Iterable[dict]
            ret = [
                {
                    k: (
                        (
                            int(str(v)[4:-1], 16)
                            if "\\x" in str(v)  # read as hexadecimal if b'\xXX'
                            else str(v)[2:-1]  # return it as str otherwise
                        )
                        if type(v) is bytes
                        else v  # if it's not a `bytes`, do nothing
                    )
                    for k, v in i.items()
                }
                for i in ret
            ]

        return ret


class MysqldbTranslator(AbstractMySqlTranslator):
    def escape_string(self, string: str) -> str:
        """Returns the given string with any suspect values handled"""
        return MySQLdb.escape_string(string).decode()

    def interpret(
        self,
        results: Union[Dict[str, Any], List[Tuple[Any]], List[Dict[str, Any]]],
        method: str,
        table: Union[str, Table],
        *args,
        **kwargs,
    ) -> Union[int, Iterable[Union[Any, Mapping[str, Any]]]]:
        """
        Given the result and its corresponding call, return the result with correct types and references

        Parameters
        ----------
        results : Any
                the results of the call defined by the rest of the parameters
        str @method is the SQL method argument. Use 'distinct' for `select distinct`
                Supported: 'select', 'delete', 'update', 'insert', 'distinct', 'count', 'show', 'describe',
                                   'add/create [temporary] table [if not exists / clobber]', 'drop [temporary] table',
                                   'rename table', 'truncate [table]', 'add/create column', 'drop column', 'alter column'
        str @table is the table name or the Table itself
                For @method = 'create table', must not be a Table or table name in self.db
                Otherwise, must be a Table in self.db.tables or a str in self.db.table_names

        Returns
        -------
        int
                for method == 'count'
        List[str]
                for method in ['show', 'describe']
        List[Union[Tuple[Any], Dict[str, Any]]]
                for method in ['select', 'distinct']
        None
                otherwise
        """
        if results is None:
            # method in ['update', 'delete', 'insert', 'create table', 'drop table', 'rename table', 'truncate table',
            # 			 'add column', 'drop column', 'alter column']
            return None

        if method in ["select", "distinct"]:
            # (tuple[, tuple]) or (dict[, dict])
            results = list(results)

        if method == "count":
            if "how" in kwargs and kwargs["how"] == 0:
                # ((n,))
                results = results[0][0]
            else:
                # ({'count':n})
                results = list(results[0].values())[0]

        elif method == "describe":
            if "how" in kwargs and kwargs["how"] == 0:
                # ( (Field, Type, Null, Key, Default, Extra) )
                results = [f"`{r[0]}` {r[1]}" for r in results]
            elif "how" in kwargs and kwargs["how"] == 2:
                # ( {COLUMNS.key : value} )
                results = [
                    f"`{r['COLUMNS.Field']}` {r['COLUMNS.Type']}" for r in results
                ]
            else:
                # ( {key : value} )
                results = [f"`{r['Field']}` {r['COLUMNS.Type']}" for r in results]
            sql += ")"

        elif "show" in method and "tables" in table:
            if "how" in kwargs and kwargs["how"] == 0:
                # ( (value,)[, (value,)] )
                results = [r[0] for r in results]
            else:
                # ( {fake: value}[, {fake: value}] )
                results = [v for r in results for v in r.values()]

        return results
