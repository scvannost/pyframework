import abc
from typing import Any, Iterable, Mapping, Type, Union

from .AbstractDatabase import (
    AbstractSqlDatabase,
    AbstractSqlTranslator,
    AbstractTranslator,
)
from .Table import Column, Table


class AbstractMySqlDatabase(AbstractSqlDatabase):
    """
    An object that represents a MySQL database and allows for interaction with it

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
    Translator : Type[AbstractTranslator] = AbstractMySqlTranslator
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
    *connect()
        connects to the database
    commit()
        commits changes to the database
    *db_query(q, limit, kwargs)
        queries the database for q
        additional kwargs ['maxrows', 'how'] passed to self.db.fetch_row
    get_table()
        gets the given table gracefully
    get_column()
        gets the given column gracefully
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
            Translator = AbstractMySqlTranslator
        super().__init__(*args, Translator=Translator, **kwargs)

    def add_fk(
        self, name: str, table: str, field: str, ref: str
    ) -> "AbstractMySqlDatabase":
        """
        Adds a foreign key constraint

        Parameters
        ----------
        name : str
            the name of the fk
        table : str
            the name of the table
        field : str
            the name of the column
        ref : str
            the table and column that is the key
            in the form: `table`(`col`)

        Returns
        -------
        self
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        sql = "ALTER TABLE " + table + " ADD CONSTRAINT fk_" + name
        sql += " FOREIGN KEY (" + field + ") REFERENCES " + ref + ";"
        self.db_query(sql)
        return self

    def drop_fk(self, name: str, table: str) -> "AbstractMySqlDatabase":
        """
        Drop a foreign key constraint

        Parameters
        ----------
        name : str
            the name of the fk
        table : str
            the name of the table

        Returns
        -------
        self
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        sql = "ALTER TABLE " + table + " DROP FOREIGN KEY fk_" + name + ";"
        self.db_query(sql)
        return self


class AbstractMySqlTranslator(AbstractSqlTranslator):
    """
    Provides validation and translation for MySql

    Parameters
    ----------
    db
        the Database this translator is used for
    *args, **kwargs
        all silently ignored
        for convenience in subclassing

    Properties
    ----------
    db : Database
        the Database given upon init
    dtypes : list(str)
        the acceptable types for this database
    NULL : str
        the database command for the null value

    Methods
    -------
    *escape_string()
        returns a database-safe version of a string
    *interpret(results, *args, **kwargs)
        takes results of self.db.db_query and marshalls them into desired returns
    translate(*args, **kwargs)
        turns the given call into the direct query for self.db.db_query
    validate(*args, **kwargs)
        returns bool of if given call passes validation or not
        calls self.validate_and_raise(*args, **kwargs)
    validate_and_raise(*args, **kwargs)
        raises any errors found invalidating the call

    Additional methods
    ------------------
    add(), div(), mul(), sub()
        returns a string representing the operation given
    contains(), eq(), ge(), gt(), like(), logical_and(), logical_or(), lt(), le(), ne()
        returns a string representing the comparison given
    groupby(), join(), limit(), orderby(), where()
        returns a string representing the substatement given
    """

    @property
    def NULL(self):
        return "NULL"

    @property
    def dtypes(self):
        return [k.lower().split("[")[0] for k in MYSQL_TYPES.keys()] + self._dtypes_multi

    @property
    def _dtypes_multi(self):
        return [k.lower().split("(")[0] for k in MYSQL_MULTI_TYPES.keys()]
    

    @abc.abstractmethod
    def interpret(
        self, results: Any, method: str, *args, **kwargs
    ) -> Union[int, Iterable[Union[Any, Mapping[str, Any]]]]:
        """
        Given the result and its corresponding call, return the result with correct types and references

        Parameters
        ----------
        results : Any
            the results of the call defined by the rest of the parameters
        str @method is the SQL method argument. Use 'distinct' for `select distinct`
            Supported: 'select', 'delete', 'update', 'insert', 'distinct', 'count', 'show', 'describe',
                       'add/create [temporary] table [if not exists / clobber]', 'drop [temporary] tables',
                       'rename table', 'truncate [table]', 'add/create column', 'drop column'

        Returns
        -------
        int
            for method == 'count'
        List[str]
            for method in ['show tables', 'describe']
        List[Union[Tuple[Any], Dict[str, Any]]]
            for method in ['select', 'distinct']
        None
            otherwise
        """
        pass

    def validate_and_raise(
        self,
        method: str,
        table: Union[str, Table],
        fields: Union[
            None,
            Mapping[Union[str, Column], Any],
            Iterable[str],
            Iterable[Column],
            str,
            Column,
            Table,
        ] = None,
        *,
        where: Union[str, Column] = None,
        limit: int = None,
        groupby: Union[str, Column] = None,
        orderby: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Take all of the args and kwargs given to self.translate
        Raise any Exception along the way to signal improper args, kwargs

        Parameters
        ----------
        str @method is the SQL method argument. Use 'distinct' for `select distinct`
            Supported: 'select', 'delete', 'update', 'insert', 'distinct', 'count', 'show', 'describe',
                       'add/create [temporary] table [if not exists / clobber]', 'drop [temporary] tables',
                       'rename table', 'truncate [table]', 'add/create column', 'drop column', 'alter column'
        str @table is the table name or the Table itself
            For @method = 'create table', must not be a Table or table name in this database
            Otherwise, must be a Table in self.tables or a str in self.table_names
        obj @fields
            Not required for @method in ['count', 'delete', 'describe', 'drop table', 'truncate']
            Not required for @method = 'create table' if @table is a Table
            'show' for @method='tables'
            Must be a Mapping of {field: value} for method in ['insert', 'update']
            Must be a str, Iterable(col) for method = 'create table' if @table is not a Table
            Must be a str for method = 'rename table'
            Must be str or Column for method in ['add column', 'drop column']
            Must be a str, Column, or Mapping[col, col] for @method = 'alter column'
            Must be a Iterable(col) or col or 'all' otherwise
        *
        col @where
            a Column with dtype in 'comparison', generated from performing a comparison on a Column
        int @limit
        col @groupby
        col @orderby
        all args, listed kwargs passed through to self.[prepare / translate]

        Additional kwargs
        -----------------
        temporary : bool = False
            for @method in ['create table', 'drop table']
            whether or not the table is a temporary table
        clobber : bool = False
            for @method='create table'
            whether or not to overwrite the table if it already exists
        after : col
            for @method='add column'
            the column in @tbl after which to add the given column
            use special value "first" to add the column as the first in the table
        to : col
            for @method in ['alter column', 'rename table']
            the new column specification for the column
            only needed if @fields is not Mapping


        Returns
        -------
        None

        Raises
        ------
        Exception, TypeError, ValueError
            if the given args, kwargs cannot be validated
        """
        if not self.db.open:
            raise Exception("You must initiate the connection.")

        if not isinstance(method, str):
            raise TypeError("Database.query@method must be a str.")

        if (
            "show" not in method or "table" not in table
        ):  # don't check for @method='show table'
            # try to interpret table and if it belongs to db
            if not isinstance(table, (str, Table)):
                raise TypeError(
                    'Database.query@table must be a Table or str in this database (except if @method="create table")'
                )
            if (
                ("add" in method or "create" in method)
                and "table" in method
                and self.db.get_table(table) is not None
            ):
                raise ValueError(
                    'Database.query@table cannot be a table in self.tables for @method="create table"'
                )
            elif (
                "create" not in method and "add" not in method
            ) or "table" not in method:
                table = self.db.get_table(table)  # need for validation
                if table is None:
                    raise ValueError(
                        'Database.query@table must be a table in self.tables for @method!="create table"'
                    )

        # BEGIN @method / @table / @fields validation
        if method == "insert":
            if not isinstance(fields, Mapping):
                raise TypeError(
                    "Database.query@fields must be Mapping[col, value] for @method=insert."
                )
            for k in fields.keys():
                if not isinstance(k, (str, Column)):
                    raise TypeError(
                        "Database.query@fields must be Mapping[col, value] for @method=insert, where col is a str or Column"
                    )
                if table.get_column(k) is None:
                    raise ValueError(
                        "Database.query@fields may only contains keys in @table.columns for @method=insert"
                    )

            for c in table.columns:
                if not c.null and c.name not in fields:
                    raise ValueError(
                        "Database.query@fields must be passed all required fields in @table for @method=insert"
                    )

        elif method == "update":
            if not isinstance(fields, Mapping):
                raise TypeError(
                    "Database.query@fields must be a Mapping for @method=update."
                )
            for k in fields.keys():
                if not isinstance(k, (str, Column)):
                    raise TypeError(
                        "Database.query@fields must be Mapping[col, value] for @method=update, where col is a str or Column"
                    )
                elif table.get_column(k) is None:
                    raise ValueError(
                        "Database.query@fields may only contains keys in @table.columns for @method=update"
                    )

        elif "rename" in method and "table" in method:
            if fields is None and "to" in kwargs:  # needed for validation
                fields = kwargs["to"]  # easy swap

            if not isinstance(fields, str):
                raise TypeError(
                    'Database.query@fields must be a str for @method="rename table"'
                )
            elif self.db.get_table(fields) is not None:
                raise ValueError(
                    'Database.query@fields must not be an existing table for @method="rename table"'
                )

        elif "drop" in method and "column" in method:
            if not isinstance(fields, (str, Column)):
                raise TypeError(
                    'Database.query@fields must be a column in @table for @method="drop column"'
                )
            if table.get_column(fields) is None:
                raise ValueError(
                    'Database.query@fields must be a column in @table for @method="drop column"'
                )

        elif ("create" in method or "add" in method) and "table" in method:
            if isinstance(table, str):
                if isinstance(fields, str):
                    fields = [
                        f.strip() for f in fields.split(",")
                    ]  # needed for validation
                    table = self.escape_string(table)  # just in case

                    used_names = []
                    for f in fields:
                        if not self.is_valid_column(Table("", table, []), f):
                            raise ValueError(
                                'Database.query@fields must be a str or Iterable(col) for @method="create table" unless @table is a Table'
                            )
                        elif isinstance(f, str) and f.rsplit(" ", 1)[0] in used_names:
                            raise ValueError(
                                'Database.query@fields may not contain duplicate column names for @method="create table" unless @table is a Table'
                            )
                        elif isinstance(f, Column) and f.name in used_names:
                            raise ValueError(
                                'Database.query@fields may not contain duplicate column names for @method="create table" unless @table is a Table'
                            )
                        else:
                            used_names.append(
                                f.name if isinstance(f, Column) else f.rsplit(" ", 1)[0]
                            )

                elif isinstance(fields, Iterable):
                    used_names = []
                    for f in fields:
                        if not self.is_valid_column(Table("", table, []), f):
                            raise ValueError(
                                'Database.query@fields must be a str or Iterable(col) for @method="create table" unless @table is a Table'
                            )
                        elif isinstance(f, str) and f.rsplit(" ", 1)[0] in used_names:
                            raise ValueError(
                                'Database.query@fields may not contain duplicate column names for @method="create table"'
                            )
                        elif isinstance(f, Column) and f.name in used_names:
                            raise ValueError(
                                'Database.query@fields may not contain duplicate column names for @method="create table"'
                            )
                        else:
                            used_names.append(
                                f.name if isinstance(f, Column) else f.rsplit(" ", 1)[0]
                            )

                else:
                    raise TypeError(
                        'Database.query@fields must be a str or Iterable(col) for @method="create table" unless @table is a Table'
                    )

            elif fields is not None:  # table is a Table
                raise ValueError(
                    'Database.query@fields must be None for @method="create table" and @table is a Table'
                )

        elif ("add" in method or "create" in method) and "column" in method:
            if not self.is_valid_column(table, fields):
                raise ValueError(
                    "Database.query@fields must be a valid column definition for the table you are adding it to"
                )

            if "after" in kwargs:
                if not self.db.get_column(table, kwargs["after"]) and not (
                    isinstance(kwargs["after"], str) and kwargs["after"] == "first"
                ):
                    raise ValueError(
                        'Database.query@after must be a column in the table or "first" if given'
                    )

        elif "alter" in method and "column" in method:
            where, groupby, orderby, limit = None, None, None, None

            if isinstance(fields, Mapping):
                if len(fields) != 1:
                    raise ValueError(
                        'Database.query@fields must have only a single key for @method="alter column" if @fields is a dict'
                    )
                elif "to" in kwargs and kwargs["to"] is not None:
                    raise ValueError(
                        'Database.query@to should not be specified for @method="alter column" if @fields is a dict'
                    )

                kwargs["to"] = list(fields.values())[0]  # needed for validation
                fields = list(fields.keys())[0]  # also this

            if not table.get_column(fields):
                raise ValueError(
                    'Database.query@fields must be a column in @table for @method="alter column"'
                )

            if "to" not in kwargs:
                raise ValueError(
                    'Database.query@to must be provided for @method="alter column"'
                )
            elif not self.is_valid_column(table, kwargs["to"]):
                raise ValueError(
                    "Database.query@to must be a valid column definition for the table"
                )

        elif method in ["distinct", "select"]:
            if not isinstance(fields, Iterable):  # need for validation
                fields = [fields]

            if not isinstance(fields, str):
                for n in range(len(fields)):
                    if table.get_column(fields[n]) is None:
                        raise TypeError(
                            "Database.query@fields must be 'all' or Iterable(col)."
                        )

            elif fields != "all" and table.get_column(fields) is None:
                raise ValueError(
                    "Database.query@fields must be 'all' or Iterable(col)."
                )

        elif method == "delete":
            fields, groupby, orderby, limit = (
                None,
                None,
                None,
                None,
            )

        elif "drop" in method and "table" in method:
            fields, where, groupby, orderby, limit = None, None, None, None, None
            # only need to check table, which has already happened

        elif "show" in method and "table" in table:
            fields, where, groupby, orderby, limit = None, None, None, None, None

        elif method in ["describe", "truncate"]:
            fields, where, groupby, orderby, limit = None, None, None, None, None

        else:
            raise ValueError(
                f"See documentation for allowed values of Database.query@method; {method!s} not allowed"
            )
        # END @method / @table / @fields validation

        if where and not (isinstance(where, Column) and where.dtype == "comparison"):
            raise TypeError(
                'Database.query@where must be a Column with dtype "comparison"'
            )

        if limit:
            if not isinstance(limit, int):
                raise TypeError("Database.query@limit must be a positive int")
            elif limit <= 0:
                raise ValueError("Database.query@limit must be a positive int")

        for name, check in {"groupby": groupby, "orderby": orderby}.items():
            if check:
                if not isinstance(check, (str, Column)):
                    raise TypeError(f"Database.query@{name} must be a column in @table")
                elif check not in table.columns + table.column_names + [
                    table.name + "." + c for c in table.column_names
                ]:
                    raise ValueError(
                        f"Database.query@{name} must be a column in @table"
                    )

    def translate(
        self,
        method: str,
        table: Union[str, Table],
        fields: Union[
            None,
            Mapping[Union[str, Column], Any],
            Iterable[Union[str, Column]],
            str,
            Column,
            Table,
        ] = None,
        *,
        where: Union[str, Column] = None,
        limit: int = None,
        groupby: Union[str, Column] = None,
        orderby: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Translate the given call into a MySQl query string
        No validation is done, use self.validate

        Parameters
        ----------
        str @method is the MySQL method argument. Use 'distinct' for `select distinct`
            Supported: 'select', 'delete', 'update', 'insert', 'distinct', 'count', 'show tables', 'describe',
                       'add/create [temporary] table [if not exists / clobber]', 'drop [temporary] tables',
                       'rename table', 'truncate [table]', 'add/create column', 'drop column', 'alter column'
        str @table is the table name or the Table itself
            For @method = 'create table', must not be a Table or table name in this database
            Otherwise, must be a Table in self.tables or a str in self.table_names
        obj @fields
            Not required for @method in ['count', 'delete', 'show tables', 'describe', 'drop table', 'truncate']
            Not required for @method = 'create table' if @table is a Table
            Must be a Mapping of {field: value} for method in ['insert', 'update']
            Must be a str, Iterable(col) for method = 'create table' if @table is not a Table
            Must be a str for method = 'rename table'
            Must be str or Column for method in ['add column', 'drop column']
            Must be a str, Column, or Mapping[col, col] for @method = 'alter column'
            Must be a Iterable(col) or col or 'all' otherwise
        *
        col @where
                a Column with dtype in 'comparison', generated from performing a comparison on a Column
        int @limit
        col @groupby
        col @orderby
        all args, listed kwargs passed through to self.[prepare / translate]

        Additional kwargs
        -----------------
        temporary : bool = False
            for @method in ['create table', 'drop table']
            whether or not the table is a temporary table
        clobber : bool = False
            for @method='create table'
            whether or not to overwrite the table if it already exists
        after : col
            for @method='add column'
            the column in @tbl after which to add the given column
            use special value "first" to add the column as the first in the table
        to : col
            for @method in ['alter column', 'rename table']
            the new column specification for the column
            only needed if @fields is not Mapping

        Returns
        -------
        str
                the query ready to pass to self.db.db_query
        """
        if (
            ("create" not in method and "add" not in method) or "table" not in method
        ) and ("show" not in method or "tables" not in table):
            table = self.db.get_table(table)

        if method == "insert":
            where, groupby, orderby, limit = None, None, None, None

        elif method == "update":
            groupby, orderby, limit = None, None, None

        elif "rename" in method and "table" in method:
            where, groupby, orderby, limit = None, None, None, None
            if fields is None and "to" in kwargs:
                fields = kwargs["to"]

        elif "drop" in method and "table" in method:
            where, groupby, orderby, limit = None, None, None, None

            if "temporary" in method or "temporary" not in kwargs:
                kwargs["temporary"] = "temporary" in method  # default to not temporary

            method = "drop table"

        elif "drop" in method and "column" in method:
            where, groupby, orderby, limit = None, None, None, None
            fields = table.get_column(fields)

        elif "truncate" in method:
            fields, where, groupby, orderby, limit = None, None, None, None, None

        elif ("create" in method or "add" in method) and "table" in method:
            where, groupby, orderby, limit = None, None, None, None

            if "temporary" in method or "temporary" not in kwargs:
                kwargs["temporary"] = "temporary" in method  # default to not temporary

            if (
                any(
                    [
                        i in method
                        for i in [
                            "if not exists",
                            "ifnot exists",
                            "if notexists",
                            "ifnotexists",
                        ]
                    ]
                )
                or "clobber" not in kwargs
            ):
                kwargs["clobber"] = False  # default to don't clobber

            if isinstance(table, Table):
                fields = table.columns
            else:  # str
                if isinstance(fields, str):
                    fields = [self.escape_string(f.strip()) for f in fields.split(",")]
                table = self.escape_string(table)

        elif ("add" in method or "create" in method) and "column" in method:
            where, groupby, orderby, limit = None, None, None, None

            if isinstance(fields, str):
                fields = self.escape_string(fields)
            else:  # isinstance(fields, Column)
                fields._name = self.escape_string(fields.name)

        elif "alter" in method and "column" in method:
            where, groupby, orderby, limit = None, None, None, None

            if isinstance(fields, Mapping):
                kwargs["to"] = list(fields.values())[0]
                fields = list(fields.keys())[0]

            if isinstance(kwargs["to"], str):
                kwargs["to"] = self.escape_string(kwargs["to"])
            else:  # isinstance(kwargs['to'], Columns)
                kwargs["to"]._name = self.escape_string(kwargs["to"].name)

        elif method in ["distinct", "select"]:
            if not isinstance(fields, Iterable):  # str is an Iterable
                fields = [fields]

        elif method == "count":
            # TODO let count more things
            fields, orderby, limit = "*", None, None

        elif method in ["delete", "describe"]:
            fields, orderby, groupby, limit = None, None, None, None

        # elif 'show' in method and 'tables' in table:
        # 	fields, orderby, groupby, limit = None, None, None, None

        # build the query
        if method in ["select", "distinct", "count"]:
            sql = f"select {method + ' ' if method in ['distinct','count'] else ''}"
            sql += f"{'*' if fields == 'all' else ', '.join(fields)} from {table!r}"

        elif method == "delete":
            sql = f"delete from {table!r}"

        elif method == "insert":
            keys = list(fields.keys())
            values = [self.escape_string(fields[k]) for k in keys]
            keys = [repr(k) for k in keys]

            sql = f"insert into {table!r} ({', '.join(keys)}) "
            sql += "values (" + ", ".join(values) + ")"

        elif method == "update":
            sql = f"update {table!r} set " + ", ".join(
                [f"{f!r} = {self.escape_string(v)}" for f, v in fields.items()]
            )

        elif method == "describe":
            sql = f"describe {table!r}"

        elif "drop" in method and "table" in method:
            sql = (
                "drop "
                + ("temporary " if kwargs["temporary"] else "")
                + f"table {table!r} if exists"
            )

        elif "drop" in method and "column" in method:
            sql = f"alter table {table!r} drop column {fields}"

        elif ("create" in method or "add" in method) and "table" in method:
            sql = (
                "create "
                + ("temporary " if kwargs["temporary"] else "")
                + f"table {table!r}"
            )
            sql += (" if not exists" if not kwargs["clobber"] else "") + " ("
            sql += ", ".join(
                [
                    f"{f.name!r} {f.dtype!s}" if isinstance(f, Column) else str(f)
                    for f in fields
                ]
            )
            sql += ")"

        elif "rename" in method and "table" in method:
            sql = f"alter table {table!r} rename {fields!r}"

        elif "truncate" in method:
            sql = f"truncate table {table!r}"

        elif ("add" in method or "create" in method) and "column" in method:
            sql = f"alter table {table!r} add column "
            sql += (
                f"{fields.name!r} {fields.dtype!s}"
                if isinstance(fields, Column)
                else str(fields)
            )
            if "after" in kwargs:
                if kwargs["after"].lower() == "first":
                    sql += " first"
                else:
                    sql += f" after {kwargs['after']!r}"

        elif "alter" in method and "column" in method:
            sql = f"alter table {table!r} change column {fields!r} "
            sql += (
                f"{kwargs['to'].name!r} {kwargs['to'].dtype!s}"
                if isinstance(kwargs["to"], Column)
                else str(kwargs["to"])
            )
        elif "show" in method and "tables" in table:
            sql = "show tables"

        else:
            raise Exception(method)

        # handle extras
        if where is not None:
            sql += self.where(where)
        if groupby is not None:
            sql += self.groupby(groupby)
        if orderby is not None:
            sql += self.orderby(orderby)
        if limit is not None:
            sql += self.limit(limit)

        return sql + ";"

    def groupby(self, groupby: Union[str, Table]) -> str:
        return f" group by {groupby!r}"

    def join(
        self,
        one: Union[str, Table],
        two: Union[str, Table],
        on: Union[str, Column],
        direction: str,
        *,
        alias: str = "",
    ) -> str:
        return (
            f"{one.name} {direction.lower()} join {two.name} "
            + (f"as {alias!r} " if alias else "")
            + f"on {on!r}"
        )

    def limit(self, limit: int) -> str:
        return f" limit {limit!r}"

    def orderby(self, orderby: Union[str, Column]) -> str:
        return f" order by {orderby!r}"

    def where(self, comparison: Column) -> str:
        return f" where {comparison!r}"


MYSQL_TYPES = {
    "BIT[(M)]": "an M bit number; default M=1",
    "TINYINT[(M)] [UNSIGNED]": "a number in the range [-128,127]; or [0,255] if unsigned",
    "BOOL": "same as TINYINT(1); 0 is FALSE, non-zero is TRUE",
    "SMALLINT[(M)] [UNSIGNED]": "a number in the range [-32768,32767]; or [0,65535] if unsigned",
    "MEDIUMINT[(M)] [UNSIGNED]": "a number in the range [-8388608,8388607]; or [0,16777215] if unsigned",
    "INT[(M)] [UNSIGNED]": "a number in the range [-2147483648,2147483647]; or [0,4294967295] if unsigned",
    "BIGINT[(M)] [UNSIGNED]": "a number in the range [-9223372036854775808,9223372036854775807]; or [0,18446744073709551615] if unsigned",
    "SERIAL": "same as BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE",
    "DECIMAL[(M[,D])] [UNSIGNED]": "an M digit number with D digits after the decimal point; default M=10, D=0; max M=65, D=30",
    "FLOAT[(M,D)] [UNSIGNED]": "an M digit number with D digits after the decimal point; default M,D = max; accurate to ~7 decimal places",
    "DOUBLE[(M,D)] [UNSIGNED]": "an M digit number with D digits after the decimal point; default M,D = max; accurate to ~15 decimal places",
    "DATE": "generic date in the range 1000-01-01 to 9999-12-31",
    "DATETIME[(fsp)]": "a datetime in the range 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999; fsp is the precision of fractional seconds, default = 0",
    "TIMESTAMP[(fsp)]": "a UTC timestamp in the range 1970-01-01 00:00:01.000000 to 2038-01-19 03:14:07.999999; fsp is the precision of fractional seconds, default = 0",
    "TIME[(fsp)]": "a time in the range -838:59:59.000000 to 838:59:59.000000; fsp is the precision of fractional seconds, default = 0",
    "YEAR[(4)]": "a 4-digit year in the range 1901 to 2155",
    "CHAR[(M)]": "a M-length string right-padded with spaces; M in [0,255], default = 1",
    "VARCHAR(M)": "a variable-length string with max length M; M in range [0,65535]",
    "BINARY[(M)]": "a CHAR[(M)] stored in binary",
    "VARBINARY(M)": "a VARCHAR(M) stored in binary",
    "TINYBLOB": "up to 255 bytes",
    "TINYTEXT": "a string stored as up to 255 bytes",
    "BLOB": "up to 65535 bytes",
    "TEXT": "a string stored as up to 65535 bytes",
    "MEDIUMBLOB": "up to 16777215 bytes",
    "MEDIUMTEXT": "a string stored as up to 16777215 bytes",
    "LONGBLOB": "up to 42942967295B = 4GB",
    "LONGTEXT": "a string stored as up to 42942967295B = 4GB"}
MYSQL_MULTI_TYPES = {
    "ENUM('value1','value2',...)": "one value from the list; max length of a value is 255 char or 1020 bytes",
    "SET('value1','value2',...)": "a set of up to 64 values from the list; max length of a value is 255 char or 1020 bytes",
}
