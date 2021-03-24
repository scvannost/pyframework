import abc
import gc
import os

from typing import Any, Iterable, List, Mapping, Tuple, Type, Union

from .Table import Column, Table


class AbstractDatabase(abc.ABC):
    """
    An object that represents a database and allows for interaction with it

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
    Translator : Type[AbstractTranslator] = AbstractDatabase
        the Translator type for this database
    **kwargs
        ignored gracefully

    Properties
    ----------
    db : Any
        the actual database object
    open : bool
        whether or not the database connection is open
    tables : List[Any]
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
    *db_query(q, limit, kwargs)
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

    Passthrough Methods
    -------------------
    add(), div(), mul(), sub()
        route to self.translator.[func]()
    contains(), eq(), ge(), gt(), like(), logical_and(), logical_or(), lt(), le(), ne()
        route to self.translator.[func]()
    """

    def __init__(
        self,
        location: str,
        user: str,
        password: str,
        name: str,
        *args,
        Translator: Type["AbstractTranslator"],
        **kwargs,
    ) -> None:
        self._location = location
        self._user = user
        self._password = password
        self._name = name
        self._db = None
        self._tables = []
        self._translator = Translator(self)

    @property
    def db(self) -> Any:
        return self._db

    @property
    def name(self) -> str:
        return self._name

    @property
    def open(self) -> bool:
        """Returns whether or not the db connection is open."""
        return (not self.db is None) and bool(self.db.open)

    @property
    def tables(self) -> List[Any]:
        """Returns the Iterable of Table objects for the db"""
        return self._tables

    @property
    def table_names(self) -> List[str]:
        """Returns a Iterable of the names of the Table objects in the db"""
        return [i.name if hasattr(i, "name") else str(i) for i in self.tables]

    @property
    def translator(self) -> "AbstractTranslator":
        """For translating queries"""
        return self._translator

    def close(self) -> "AbstractDatabase":
        """
        Closes the db if it's open
        Does nothing if the db is not open
        """
        if self.open:
            for i in self.table_names:
                delattr(self, i)
            self._tables = None

            self.db.close()
            self._db = None
            gc.collect()
        return self

    @abc.abstractmethod
    def connect(self) -> "AbstractDatabase":
        """
        Connects to the database
        If a connection is already open, does nothing

        1. Opens a new connection to the db
        2. Attach to self via setattr using the table name
        3. Set up _table as Iterable[getattr(self, table)]
        4. Return self
        """
        pass

    def get_column(self, table: Any, column: Any) -> Any:
        """
        Gets the given column from the given table if both exist
        Equivalent to self.get_table(table).get_column(column) but handles non-existent table

        Parameters
        ----------
        table : Any
        column : Any
            the table and column to look for

        Returns
        -------
        None
            if self.get_table(table) returns None
            if found table doesn't hasattr "get_column"
        Any
            self.get_table(table).get_column(column)
        """
        table = self.get_table(table)
        if table is None or not hasattr(table, "get_column"):
            return None
        else:
            return table.get_column(column)

    def get_table(self, table: Any) -> Table:
        """
        Gets the given table if it exists

        Parameters
        ----------
        table : Any
            the table to look for

        Returns
        -------
        None
            if table not in self.tables and table.db is not self
            if table : str not in self.table_names
        @table
            if not in self.tables but hasattr "db" and table.db is self
        Any
            the table from self.tables
        """
        if table in self.tables or (hasattr(table, "db") and table.db == self):
            return table
        elif isinstance(table, str) and table in self.table_names:
            return self.tables[self.table_names.index(table)]
        else:
            return None

    def interpret(self, results: Any, *args, **kwargs) -> Any:
        """
        Takes the results of self.db_query and the same args as self.translate
        Returns the results in the correct format
        """
        return self.translator.interpret(results, *args, **kwargs)

    def is_valid_column(self, table: Any, column: Any, find_table: bool = True) -> bool:
        """
        Returns if column definition is valid in the current database for the given table

        Parameters
        ----------
        table : Any
        column : Any
            the table / column to validate
        find_table : bool = True
            whether to pass @table through self.get_table
            set to False if the table is not in this database

        Returns
        -------
        False
            if @table is None, after self.get_talbe if @find_table
        bool
            self.translator.is_valid_column(table, column)
        """
        if find_table:
            table = self.get_table(table)
        if not table:
            return False

        return self.translator.is_valid_column(table, column)

    @abc.abstractmethod
    def query(self, *args, **kwargs) -> Any:
        """
        Some way to make calls on the database

        1. Use self.validate or self.validate_and_raise to validate the call
        2. Use self.translate to generate the database code
        3. Query the underlying database object self.db
        4. Use self.interpret to marshall any database response
        5. Return any response
        """
        pass

    def reconnect(self) -> "AbstractDatabase":
        """
        Makes sure the db is open
        If the db is already open, it closes and connects again
        Returns self
        """
        self.close()
        return self.connect()

    def translate(self, *args, **kwargs) -> str:
        """
        Translates the given call into code that can be executed by underlying database instance without validation
        Use self.validate to see if a given call to this function will produce valid code
        """
        return self.translator.translate(*args, **kwargs)

    def validate(self, *args, **kwargs) -> bool:
        """
        Takes the same args as self.translate
        Returns true if they are a valid call
        """
        return self.translator.validate(*args, **kwargs)

    def validate_and_raise(self, *args, **kwargs) -> None:
        """
        Takes the same args as self.translate
        Raises an exception if there's any issues
        """
        return self.translator.validate_and_raise(*args, **kwargs)

    def add(self, a: Any, b: Any) -> Any:
        return self.translator.add(a, b)

    def contains(self, a: Any, b: Any) -> Any:
        return self.translator.contains(a, b)

    def div(self, a: Any, b: Any) -> Any:
        return self.translator.div(a, b)

    def eq(self, a: Any, b: Any) -> Any:
        return self.translator.eq(a, b)

    def ge(self, a: Any, b: Any) -> Any:
        return self.translator.ge(a, b)

    def gt(self, a: Any, b: Any) -> Any:
        return self.translator.gt(a, b)

    def le(self, a: Any, b: Any) -> Any:
        return self.translator.le(a, b)

    def like(self, a: Any, b: Any) -> Any:
        return self.translator.like(a, b)

    def logical_and(self, a: Any, b: Any) -> Any:
        return self.translator.logical_and(a, b)

    def logical_or(self, a: Any, b: Any) -> Any:
        return self.translator.logical_or(a, b)

    def lt(self, a: Any, b: Any) -> Any:
        return self.translator.lt(a, b)

    def mul(self, a: Any, b: Any) -> Any:
        return self.translator.mul(a, b)

    def ne(self, a: Any, b: Any) -> Any:
        return self.translator.ne(a, b)

    def sub(self, a: Any, b: Any) -> Any:
        return self.translator.sub(a, b)


class AbstractTranslator(abc.ABC):
    """
    Provides validation and translation for a particular database dialect

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
    *dtypes : list(str)
        the acceptable types for this database
    *NULL : str
        the database command for the null value

    Methods
    -------
    *escape_string()
        returns a database-safe version of a string
    *interpret(results, *args, **kwargs)
        takes results of self.db.db_query and marshalls them into desired returns
    *translate(*args, **kwargs)
        turns the given call into the direct query for self.db.db_query
    validate(*args, **kwargs)
        returns bool of if given call passes validation or not
        calls self.validate_and_raise(*args, **kwargs)
    *validate_and_raise(*args, **kwargs)
        raises any errors found invalidating the call


    Additional Methods
    ------------------
    *add(), *div(), *mul(), *sub()
    *contains(), *eq(), *ge(), *gt(), *like(), *logical_and(), *logical_or(), *lt(), *le(), *ne()
    *groupby(), *join(), *limit(), *orderby(), *where()
    """

    def __init__(self, db: AbstractDatabase, *args, **kwargs) -> None:
        self._db = db

    @property
    def db(self) -> AbstractDatabase:
        return self._db

    @property
    @abc.abstractmethod
    def dtypes(self) -> List[str]:
        """The acceptable types for this database, including the name of multi dtypes"""
        pass

    @property
    @abc.abstractmethod
    def _dtypes_multi(self) -> List[str]:
        """The acceptable dtypes that consistent of fixed set of values"""
        pass
    

    @property
    @abc.abstractmethod
    def NULL(self) -> str:
        """The NULL value of this database"""
        pass

    @abc.abstractmethod
    def escape_string(self, string: str) -> str:
        """
        Returns the given string with any suspect values handled
        """
        pass

    def is_valid_column(self, table: Any, col: Any) -> bool:
        """
        Returns if column definition is valid in the current database for the given table
        If not @find_method: str, table: Union[str, Table], fields: Union[None, Mapping[Union[str, Column], Any], Iterable[str], Iterable[Column], str, Column, Table] = None, *, where : Union[str, Column] = None, limit : int = None, groupby : Union[str, Column] = None, orderby : Union[str, Column] = None, **kwargs):table, assumes that @table is a Table
        """
        if isinstance(col, str):
            if len(col.split(" ", 1)) < 2:
                print("str len")
                return False
            elif col.rsplit(" ", 1)[1] not in self.dtypes:
                print("str dtype")
                return False
            elif table.get_column(col.rsplit(" ", 1)[0]) is None:
                return True
            else:
                print()
                print(table.columns)
                print(table.get_column(col.rsplit(" ", 1)[0]))
                print("str else")
                return False
        elif hasattr(col, "dtype"):
            if col.dtype not in self.dtypes:
                print("col dtype")
                return False
            elif table.get_column(col) is None:
                return True
            else:
                print("col else")
                return False
        else:
            print("else")
            return False


    @abc.abstractmethod
    def interpret(self, results: Any, *args, **kwargs) -> Union[Any, None]:
        """
        Given the result and its corresponding call, return the result with correct types
        """
        pass

    @abc.abstractmethod
    def translate(self, *args, **kwargs) -> str:
        """
        Parameters
        ----------
        args : tuple
            the args from self.validate, after validation and modification
        kwargs : dict
            the kwargs from self.validate, after validation and modification

        Returns
        -------
        str
            the query ready to pass to self.db.db_query
        """
        pass

    def lstrip_dtype(self, definition: str) -> Tuple[str, str]:
        """
        Parameters
        ----------
        definition : str
            begins with a valid dtype

        Returns
        -------
        str
            the dtype, properly hanlding those in _dtypes_multi
        str
            the rest of the definition
        """
        if len(definition.split()) == 1:
            raise ValueError(f"Cannot strip a dtype from a single word: {definition}")

        dtype, definition = definition.split(None, 1)

        for multi_dtype in self._dtypes_multi:
            # for each multi type, test, and break out when we've done it
            if dtype.startswith(multi_dtype):

                # need to make sure we have more than just dtype and whitespace
                if not dtype[len(multi_dtype):].strip():
                    if len(definition.split()) == 1:
                       raise ValueError(f"Cannot find any values for dtype {multi_dtype} from: {dtype + definition}") 

                    temp, definition = definition.split(None, 1)
                    dtype += temp

                # the following non-whitespace char is the container
                brace = dtype[len(multi_dtype):].strip()[0]
                # we want through until it's opposite
                for onset, coda in ["[]", "()", "{}", "<>"]:
                    if brace == onset:
                        brace = coda
                        break

                # extend the dtype to find the end
                if brace not in dtype:

                    # but don't want to break within quotes, so track that
                    quotes = ""
                    for n in range(len(definition)):
                        # loop through the rest
                        token = definition[n]

                        # once we find it, we're done
                        if not quotes and token == brace:
                            temp, definition = definition[:n+1], definition[n+1:]
                            dtype += temp
                            break

                        # otherwise if we find a not escaped quote
                        if token in ["'", '"', "`"] and not (quotes and definition[n-1] == "/"):
                            # if it's a close quote, stop tracking
                            if quotes and quotes == token:
                                quotes = ""
                            # if it's an open quote, start tracking
                            elif not quotes:
                                quotes = tokens
                            # don't care about it otherwise
                            else:
                                pass

                # once we're done processing, don't need to check the rest of the multi dtypes
                break

        return dtype, definition

    def validate_and_raise(self, *args, **kwargs):
        """
        Validates the given args and kwargs to see if it is in the correct format for self.prepare or class.prepare_and_raise

        Parameters
        ----------
        args : tuple
            the raw args for processing to give to self.translate
        kwargs : dict
            the raw kwargs for processing to give to self.translate and self.db_query

        Returns
        -------
        None

        Raises
        ------
        Exception
            if the given args, kwargs cannot be validated
        """
        pass

    def validate(self, *args, **kwargs) -> bool:
        """
        Validates the given args and kwargs to see if it is in the correct format for self.translate

        Parameters
        ----------
        args : tuple
            the raw args for processing to give to self.translate
        kwargs : dict
            the raw kwargs for processing to give to self.translate and self.db_query

        Returns
        -------
        bool
            True if args, kwargs do not raise any errors in class.prepare_and_raise
            False if class.prepare_and_raise throws an error during validation

        Raises
        ------
        None
        """
        try:
            self.validate_and_raised(db, *args, **kwargs)
        except Exception:
            return False
        else:
            return True

    @abc.abstractmethod
    def add(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def contains(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def div(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def eq(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def ge(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def gt(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def le(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def like(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def logical_and(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def logical_or(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def lt(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def mul(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def ne(self, a: Any, b: Any) -> Any:
        pass

    @abc.abstractmethod
    def sub(self, a: Any, b: Any) -> Any:
        pass


class AbstractSqlDatabase(AbstractDatabase):
    """
    An object that represents a SQL database and allows for interaction with it

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
    Translator : Type[AbstractTranslator] = AbstractDatabase
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
    alter_column()
        alter the given column in the given table
    drop_ column()
        drop the given column from the given table
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
            Translator = AbstractSqlTranslator
        super().__init__(*args, Translator=Translator, **kwargs)

    def add_column(
        self,
        table: Union[str, Table],
        new: Union[str, Column],
        after: Union[str, Column] = None,
    ) -> Table:
        """
        Adds a column to an existing table

        Parameters
        ----------
        table : str, Table
            the table to alter
        new : str, Column
            the new column to add
        after : str, Column
            the column after which to add the new column
            special value: 'first'

        Returns
        -------
        Table
            the new table that was created
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        self.query("add column", table, new, after=after)

        columns = self.query("describe", table)
        columns = [Column.from_definition(c) for c in columns]

        if isinstance(table, Table):
            setattr(self, table.name, Table(self, table.name, columns))
            if table in self.tables:
                del self._tables[self.tables.index(table)]
            self._tables.append(getattr(self, table.name))
        else:
            setattr(self, table, Table(self, table, columns))
            if table in self.table_names:
                del self._tables[self.table_names.index(table)]
            self._tables.append(getattr(self, table))

        return self.get_table(table)

    def alter_column(
        self, table: Union[str, Table], old: Union[str, Column], new: Union[str, Column]
    ) -> Table:
        """
        Alters the given column in the given table

        Parameters
        ----------
        table: str, Table
        old : str, Column
            the the table / column to alter
        new : str, Column
            the desired column definition

        Returns
        -------
        Table
            the table that contains the new column
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        self.query("alter column", table, old, to=new)
        columns = self.query("describe", table)
        columns = [Column.from_definition(c) for c in columns]

        if isinstance(table, Table):
            setattr(self, table.name, Table(self, table.name, columns))
            if table in self.tables:
                del self._tables[self.tables.index(name)]
            self._tables.append(getattr(self, table.name))

        else:
            setattr(self, table, Table(self, table, columns))
            if table in self.table_names:
                del self._tables[self.table_names.index(table)]
            self._tables.append(getattr(self, table))

        return self.get_table(table)

    def commit(self) -> "AbstractSqlDatabase":
        """
        Commits changes to the db
        Returns self
        """
        if not self.open:
            Exception("You must initiate the connection.")
        self.db.commit()
        return self

    @abc.abstractmethod
    def db_query(self, q: str, limit: int = -1, **kwargs) -> Any:
        """
        Parameters
        ----------
        q : str
            the SQL query, ending in ';'
            no validation is done for correctness
        limit : int = -1
            is the number of results to return
            If < 0, special value for all

        Returns
        -------
        Any
            the results of the query
            default type List[Dict[str, Any]]
        None
            if no result, such as for update, delete, etc
        """
        pass

    def drop_column(self, table: str, column: str) -> Table:
        """
        Drops the given column from the given table
        Does not check if dropping the column will be a problem

        Parameters
        ----------
        str @table is the table to alter
        str @column is the column to drop

        Returns
        -------
        Table
                the new table
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        self.query("drop column", table, column)

        columns = self.query("describe", table)
        columns = [Column.from_definition(c) for c in columns]

        if isinstance(table, Table):
            setattr(self, table.name, Table(self, table.name, columns))
            if table in self.tables:
                del self._tables[self.tables.index(table)]
            self._tables.append(getattr(self, table.name))
        else:
            setattr(self, table, Table(self, table, columns))
            if table in self.table_names:
                del self._tables[self.table_names.index(table)]
            self._tables.append(getattr(self, table))

        return self.get_table(table)

    def drop_table(
        self, table: Union[str, Table], temporary: bool = False
    ) -> "AbstractSqlDatabase":
        """
        Drops the given table

        Parameters
        ----------
        tabl @name is the table to drop
        bool @temporary to specify only drop TEMPORARY tables; default False

        Returns
        -------
        self
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        self.query("drop table", table, temporary=temporary)

        if isinstance(table, Table):
            del self._tables[self.tables.index(table)]
            delattr(self, table.name)
        else:
            del self._tables[self.table_names.index(table)]
            delattr(self, table)

        return self

    def join(
        self,
        one: Union[str, Table],
        two: Union[str, Table],
        on: Union[str, Column],
        direction: str,
        *,
        alias: str = "",
    ) -> Table:
        return self.translator.join(one, two, on, direction, alias=alias)

    def make_table(
        self,
        table: Union[str, Table],
        columns: Union[str, Column, Mapping[str, str], Iterable[Union[str, Column]]],
        temporary: bool = False,
        clobber: bool = False,
    ) -> Table:
        """
        Creates a new table in the databasename and generates its class

        Parameters
        ----------
        tbl @table is the name of the table
        @columns can be:
            str as comma-separated column definitions;
            Iterable[str] as columns definitions; or
            Mapping[str, str] as {field: definition}
        bool @temporary to make a temporary table
        bool @clobber to overwrite an existing table; default False

        Returns
        -------
        Table
                the new table
        """
        # checks
        if not self.open:
            raise Exception("You must initiate the connection.")

        # create the table
        self.query("create table", table, columns, temporary=temporary, clobber=clobber)
        columns = self.query("describe", table)
        columns = [
            Column.from_definition(c, table=table) for c in columns[0].split(", ")
        ]

        # make the Table and return it
        if isinstance(table, Table):
            setattr(self, table.name, Table(self, table.name, columns))
            self._tables.append(getattr(self, table.name))
        else:
            setattr(self, table, Table(self, table, columns))
            self._tables.append(getattr(self, table))

        return self.get_table(table)

    def move_table(self, old: Union[str, Table], new: Union[str, Table]) -> Table:
        """
        Renames the given table with the new name

        Parameters
        ----------
        tbl @old is the current table
        tbl @new is the new table

        Returns
        -------
        Table
                the new Table object
        """
        if not self.open:
            raise Exception("You must initiate the connection.")

        self.query("rename", old, new)
        columns = self.query("describe", new)
        columns = [Column.from_definition(c) for c in columns[0].split(", ")]

        # make the Table and return it
        if isinstance(old, Table):
            delattr(self, old.name)
            del self._tables[self.tables.index(old)]
        else:
            delattr(self, old)
            del self._tables[self.table_names.index(old)]

        if isinstance(new, Table):
            setattr(self, new.name, Table(self, new.name, columns))
            self._tables.append(getattr(self, new.name))
        else:
            setattr(self, new, Table(self, new, columns))
            self._tables.append(getattr(self, new))

        return self.get_table(new)

    def query(
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
    ) -> Any:
        """
        Provides the API through which you can query the database without writing SQL code yourself
        This function relies on self.prepare, self.translate, and self.db_query, which all are implemented by a subclass
        Any new subclass of AbstractSqlDatabase should be written to provide the functionality below, at a minimum
        Expanded functionality can be handled via special kwargs that are otherwise silently ignored in other functions

        Parameters
        ----------
        str @method is the SQL method argument. Use 'distinct' for `select distinct`
            Supported: 'select', 'delete', 'update', 'insert', 'distinct', 'count', 'show tables', 'describe',
                       'add/create [temporary] table [if not exists / clobber]', 'drop [temporary] tables',
                       'rename table', 'truncate [table]', 'add/create column', 'drop column'
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
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        int @limit
        col @groupby
        col @orderby
        all args, listed kwargs passed through to self.[prepare / translate]

        Returns
        -------
        Any
            if method has a return

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
        all additional kwargs passed through to self.[prepare / translate / db_query]
        """
        self.validate_and_raise(
            method,
            table,
            fields,
            where=where,
            limit=limit,
            groupby=groupby,
            orderby=orderby,
            **kwargs,
        )

        # call db_query to get results
        results = self.db_query(
            self.translate(
                method,
                table,
                fields,
                where=where,
                limit=limit,
                groupby=groupby,
                orderby=orderby,
                **kwargs,
            ),
            **kwargs,
        )

        return self.translator.interpret(
            results,
            method,
            table,
            fields,
            where=where,
            limit=limit,
            groupby=groupby,
            orderby=orderby,
            **kwargs,
        )

    def rollback(self) -> "AbstractSqlDatabase":
        """
        Rolls back any changes to the db
        Returns self
        """
        self.db.rollback()
        return self

    def truncate_table(self, table: Union[str, Table]) -> "AbstractSqlDatabase":
        """
        Truncates the given table

        Parameters
        ----------
        tbl @table is the table to truncate

        Returns
        -------
        self

        """
        if not self.open:
            raise Exception("You must initiate the connection.")
        self.query("truncate", table)
        return self


class AbstractSqlTranslator(AbstractTranslator):
    """
    Provides validation and translation for a particular database dialect

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
    *dtypes : list(str)
        the acceptable types for this database
    *NULL : str
        the database command for the null value

    Methods
    -------
    *escape_string()
        returns a database-safe version of a string
    *interpret(results, *args, **kwargs)
        takes results of self.db.db_query and marshalls them into desired returns
    *translate(*args, **kwargs)
        turns the given call into the direct query for self.db.db_query
    validate(*args, **kwargs)
        returns bool of if given call passes validation or not
        calls self.validate_and_raise(*args, **kwargs)
    *validate_and_raise(*args, **kwargs)
        raises any errors found invalidating the call

    Additional Methods
    ------------------
    add(), div(), mul(), sub()
        returns default implementations of operations
    contains(), eq(), ge(), gt(), like(), logical_and(), logical_or(), lt(), le(), ne()
        returns default implementations of comparisons
    *groupby(), *join(), *limit(), *orderby(), *where()
        returns a string representing the substatement given
    """

    # Return Column( db : Column, name : str, dtype : str in ['operation', 'comparison'])
    # custom version of Column
    def add(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} + {b!r})", "operation")

    def contains(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} in {b!r})", "comparison")

    def div(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} / {b!r})", "operation")

    def eq(self, a: Any, b: Any) -> Any:
        if not a:
            a = self.null
        if not b:
            b = self.null
        return Column((a, b), f"({a!r} = {b!r})", "comparison")

    def ge(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} >= {b!r})", "comparison")

    def gt(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} > {b!r})", "comparison")

    def le(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} <= {b!r})", "comparison")

    def like(self, a: Any, b: Any) -> Any:
        if not a:
            a = self.null
        if not b:
            b = self.null
        return Column((a, b), f"({a!r} like {b!r})", "comparison")

    def logical_and(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} and {b!r})", "comparison")

    def logical_or(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} or {b!r})", "comparison")

    def lt(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} < {b!r})", "comparison")

    def mul(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} * {b!r})", "operation")

    def ne(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} <> {b!r})", "comparison")

    def sub(self, a: Any, b: Any) -> Any:
        return Column((a, b), f"({a!r} - {b!r})", "operation")

    @abc.abstractmethod
    def groupby(self, groupby: Union[str, Table]) -> str:
        pass

    @abc.abstractmethod
    def join(
        self,
        one: Union[str, Table],
        two: Union[str, Table],
        on: Union[str, Column],
        direction: str,
        *,
        alias: str = "",
    ) -> Table:
        pass

    @abc.abstractmethod
    def limit(self, limit: int) -> str:
        pass

    @abc.abstractmethod
    def orderby(self, orderby: Union[str, Column]) -> str:
        pass

    @abc.abstractmethod
    def where(self, comparison: Column) -> str:
        pass
