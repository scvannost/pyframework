from typing import Any, Iterable, List, Mapping, Union

class Column:
    """
    An object that represents a column within a table in a database and provides a simple interface

    Parameters
    ----------
    table : Any
        the table to which this column belongs
    name : str
        the name of this column
    dtype : Any
        the type of this column
    *
    null: bool = False
        whether or not the column can take a null value
    default: Any = None
        the default value of the column if not given upon insert
    visible: bool = True
        whether the column is visible or not
    increment: bool = False
        whether or not to auto increment on each insert
    unique: bool = False
        whether or not the column must be unique
    key: bool = False
        whether or not the column is a key aka index
    primary: bool = False
        whether or not the column is the primary key
        implies @unique and @key are True
    comment: str = ''
        a comment about the column

    Properties
    ----------
    table : Any
        the table to which this column belongs
    name : str
        the name of this column
    dtype : Any
        the type of this column
    null : bool
        whether this column can take a null value
    default: Any = None
        the default value of the column if not given upon insert
    visible: bool = True
        whether the column is visible or not
    increment: bool = False
        whether or not to auto increment on each insert
    unique: bool = False
        whether or not the column must be unique
    key: bool = False
        whether or not the column is a key aka index
    primary: bool = False
        whether or not the column is the primary key
        implies @unique and @key are True
    comment: str = ''
        a comment about the column

    Methods
    -------
    __bool__
        if it's a comparison, returns whether the two columns are the same
        else returns True

    Passthrough Methods
    -------------------
    __add__, __mul__, __sub__, __truediv__
        route to self.table.db functions for operations
        delegated further to self.table.db.translator.[func]()
    __and__, __contains__, __eq__, __ge__, __gt__, __lt__, __le__, __mod__, __ne__, __or__
        route to self.table.db functions for comparisons
        delegated further to self.table.db.translator.[func]()
    """

    def __init__(
        self,
        table: Any,
        name: str,
        dtype: Any,
        *,
        null: bool = True,
        default: Any = None,
        visible: bool = True,
        increment: bool = False,
        unique: bool = False,
        key: bool = False,
        primary: bool = False,
        comment: str = "",
    ) -> None:
        self._table = table
        self._name = name
        self._dtype = dtype
        self._null = True if null else False
        self._default = default
        self._visible = True if visible else False
        self._increment = True if increment else False
        self._unique = True if unique or primary else False
        self._key = True if key or primary else False
        self._primary = True if primary else False
        self._comment = comment

    @classmethod
    def from_definition(cls, definition: str, *, table: Any = None) -> "Column":
        """

        Parameters
        ----------
        definition : str
            the definition of the column in the form (case INsensitive)
            "name dtype [[NOT] NULL] [DEFAULT value] [[IN]VISIBLE] [[AUTO_]INCREMENT]
                [UNIQUE [KEY]] [[PRIMARY] KEY] [COMMENT value]"
        *
        table: Any = None
            the table of the column
        """
        kwargs = {"table": table}
        definition = definition.strip()

        # get the name as the first word or phrase
        if definition[0] == "`":
            kwargs["name"], definition = definition[1:].split("`", 1)
            definition = definition.strip()
        else:
            kwargs["name"], definition = definition.split(None, 1) # don't need to strip for None
        kwargs["name"] = kwargs["name"].lower()

        # don't know what dtype will look like
        # but we know what could come after it, so see what the first one of those is
        options = ["null", "default", "visible", "increment", "unique", "key", "comment"]
        first_opt = options.pop(0)
        while first_opt and first_opt not in definition.lower():
            first_opt = options.pop(0) if len(options) else None

        # if there is one of them in definition
        if first_opt:
            # the dtype is before it
            dtype = definition[:definition.lower().index(first_opt)].strip()

            # but some have optional preludes from what was checked
            if first_opt == "null" and dtype.endswith(" not"):
                dtype = dtype[:-3].strip()
                kwargs["null"] = False
            elif first_opt == "visible" and dtype.endswith(" in"):
                dtype = dtype[:-2].strip()
            elif first_opt == "increment" and dtype.endswith(" auto_"):
                dtype = dtype[:-5].strip()
            elif first_opt == "key" and dtype.endswith(" primary"):
                dtype = dtype[:-7].strip()

            # the definition then continues after the dtype
            definition = definition.split(dtype, 1)[1].strip().split()
            kwargs["dtype"] = dtype


            if "default" in definition:
                start = definition.index("default") + 1  # one for name, one to get following
                end = start + 1
                while end < len(definition) and definition[end] not in [
                    "visible",
                    "invisible",
                    "auto_increment",
                    "increment",
                    "unique",
                    "key",
                    "comment",
                ]:
                    end += 1
                kwargs["default"] = " ".join(definition[start : end])

            kwargs["visible"] = "visible" in definition or "invisible" not in definition
            kwargs["increment"] = "auto_increment" in definition or "increment" in definition
            kwargs["unique"] = "unique" in definition
            kwargs["key"] = "key" in definition
            kwargs["primary"] = "primary" in definition

            if "comment" in definition:
                start = definition.index("comment") + 1
                kwargs["comment"] = " ".join(definition[start:])

        # if there's none, we're done
        else:
            kwargs["dtype"] = definition

        return cls(**kwargs)

    def __repr__(self) -> str:
        """Makes a str representation of self for queries"""
        return (
            f"{self.table.name}.{self.name}"
            if hasattr(self.table, "name")
            else self.name
        )

    def __str__(self) -> str:
        """Makes a print-friendly str of self to display"""
        return f"{self.name} {self.dtype!s}"

    @property
    def table(self) -> Any:
        """
        The Table this column is attached to
        Can be None if passed explicitly during creation
        """
        return self._table

    @property
    def name(self) -> str:
        """Returns the name of the column"""
        return self._name

    @property
    def dtype(self) -> Any:
        """
        Returns the type of the column
        Can be None if passed explicitly during creation

        Special values
        --------------
        'comparison'
            the result of using comparison methods on two columns
        'operation'
            the result of using operation methods on a column
        """
        return self._dtype

    @property
    def null(self) -> bool:
        """Returns whether or not the column can take a null value"""
        return self._null

    @property
    def default(self) -> Any:
        """The default value of the column if not given upon insert"""
        return self._default

    @property
    def visible(self) -> bool:
        """Returns whether or not this column is visible in its parent table"""
        return self._visible

    @property
    def increment(self) -> bool:
        """Returns whether or not this column auto increments"""
        return self._increment

    @property
    def unique(self) -> bool:
        """Returns whether or not the column must only contain unique entries"""
        return self._unique

    @property
    def key(self) -> bool:
        """Returns whether or not this column is a key aka index"""
        return self._key

    @property
    def primary(self) -> bool:
        """
        Returns whether or not the column is the primary key of the table
        Implies both self.key and self.unique are True
        """
        return self._primary

    @property
    def comment(self) -> str:
        """Returns the comment on this column"""
        return self._comment

    def __bool__(self) -> bool:
        # handle comparisons by doing a comparison
        if self.dtype == "comparison":
            a, b = self.table

            # if both are Columns
            if isinstance(a, type(self)) and isinstance(b, type(self)):
                return all([a.table == b.table, a.name == b.name, a.dtype == b.dtype])

            # if one is Column and other is str
            elif (isinstance(a, type(self)) and isinstance(b, str)) or (
                isinstance(a, str) and isinstance(b, type(self))
            ):
                # make b always the str
                if isinstance(a, str):
                    a, b = b, a

                # handle table names
                # be ready in case of space
                if b.split(" ")[0].count(".") == 1:
                    current = (a.table == b.split(" ")[0].split(".")[0]) and (
                        a.name == b.split(" ")[0].split(".")[1].rsplit(" ", 1)[0]
                    )  # rsplit in case of type
                else:
                    current = a.name == b

                # handle type
                if b.count(" ") > 0:
                    current = current and a.dtype == b.rsplit(" ", 1)[-1]

                return current

            # if two string, normal comparison
            elif isinstance(a, str) and isinstance(b, str):
                return a == b

            # returning False is more conservative
            else:
                return False

        # otherwise doing object test, so return true
        else:
            return True

    def __add__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "add"):
            return self.table.db.add(self, a)
        else:
            raise NotImplementedError()

    def __and__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "logical_and"):
            return self.table.db.logical_and(self, a)
        else:
            raise NotImplementedError()

    def __contains__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "contains"):
            return self.table.db.contains(self, a)
        else:
            raise NotImplementedError()

    def __eq__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "eq"):
            return self.table.db.eq(self, a)
        else:
            raise NotImplementedError()

    def __ge__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "ge"):
            return self.table.db.ge(self, a)
        else:
            raise NotImplementedError()

    def __gt__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "eq"):
            return self.table.db.eq(self, a)
        else:
            raise NotImplementedError()

    def __mod__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "like"):
            return self.table.db.like(self, a)
        else:
            raise NotImplementedError()

    def __mul__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "mul"):
            return self.table.db.mul(self, a)
        else:
            raise NotImplementedError()

    def __ne__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "ne"):
            return self.table.db.ne(self, a)
        else:
            raise NotImplementedError()

    def __or__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "logical_or"):
            return self.table.db.logical_or(self, a)
        else:
            raise NotImplementedError()

    def __le__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "le"):
            return self.table.db.le(self, a)
        else:
            raise NotImplementedError()

    def __lt__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "lt"):
            return self.table.db.lt(self, a)
        else:
            raise NotImplementedError()

    def __sub__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "sub"):
            return self.table.db.sub(self, a)
        else:
            raise NotImplementedError()

    def __truediv__(self, a):
        if hasattr(self.table, "db") and hasattr(self.table.db, "div"):
            return self.table.db.div(self, a)
        else:
            raise NotImplementedError()


class Table:
    """
    An object that represents a table in a database and provides a simple interface

    Parameters
    ----------
    db : Any
        the database to which this table belongs
    name : str
        the name of the table itself
    columns : Iterable[Columns]
        the column that belong to this table
        if column._table exists, sets it to the new table
    *
    temporary: bool = False
        whether or not this a temporary table
    increment: int = None
        the initial value for auto incrementating
    comment: str = ''
        a comment about the table


    Properties
    ----------
    db : Any
        the database to which this table belongs
    name : str
        the name of the table itself
    columns : Iterable[Columns]
        the column that belong to this table
        if column._table exists, sets it to the new table
    column_names : List[str]
        returns a list of the names of the columns
    temporary: bool = False
        whether or not this a temporary table
    increment: int = None
        the initial value for auto incrementating
    comment: str = ''
        a comment about the table

    Methods
    --------
    count()
        returns the count of the entries
    delete()
        deletes the entries
    distinct()
        returns distinct values of the entries
    insert()
        inserts entries
    join(table, on, alias, direction) - returns a Table that allows for querying on the inner/left/
        right joined with @table aliased as @alias on @on
    select()
        returns the entries
    update()
        update the entries
    """

    def __init__(
        self,
        db: Any,
        name: str,
        columns: Iterable[Column],
        *,
        temporary: bool = False,
        increment: int = None,
        comment: str = "",
    ) -> None:
        self._db = db
        self._name = name
        self._columns = columns
        self._temporary = True if temporary else False
        self._increment = increment
        self._comment = comment

        for c in self.columns:
            c._table = self
            if c.visible:
                setattr(self, c.name, c)

    def __repr__(self) -> str:
        return f"{self.name!r}" if "join" not in self.name else self.name

    def __str__(self) -> str:
        """Makes a str representation of self to display."""
        return f"Table {self.name}: " + ", ".join([str(c) for c in self.columns])

    @property
    def db(self) -> Any:
        """The db object given upon creation"""
        return self._db

    @property
    def name(self) -> str:
        """The name of the table itself"""
        return self._name

    @property
    def columns(self) -> Iterable[Column]:
        """The columns of this table"""
        return self._columns

    @property
    def column_names(self) -> List[str]:
        """A list of the column names"""
        return [i.name if hasattr(i, "name") else str(i) for i in self.columns]

    @property
    def temporary(self) -> bool:
        """Returns whether or not this table is temporary"""
        return self._temporary

    @property
    def increment(self) -> Any:
        """The initial value for auto incrementing"""
        return self._increment

    @property
    def comment(self) -> str:
        """A comment about the table"""
        return self._comment

    def get_column(self, col: Any) -> Any:
        """
        Returns the given col if it's in self.columns or col : str in self.column_names
        Otherwise None
        """
        if isinstance(col, Column) and col in self.columns:
            return col
        elif isinstance(col, str) and col in self.column_names:
            return self.columns[self.column_names.index(col)]
        elif (
            isinstance(col, str)
            and self.name in col
            and col.replace(self.name, "").strip(".") in self.column_names
        ):
            return self.columns[
                self.column_names.index(col.replace(self.name, "").strip("."))
            ]
        else:
            return None

    def count(self, where: Column = None, groupby: Column = None, **kwargs):
        """
        Runs a 'count' SQL query on the table.
        @where specifies a condition to meet.
        """
        return self.db.query("count", self.name, where=where, groupby=groupby, **kwargs)

    def delete(self, where: Column = None):
        """
        Runs a 'delete' SQL query on the table.
        @where specifies a condition to meet.
        """
        return self.db.query("delete", self.name, where=where)

    def distinct(
        self,
        fields: Union[Iterable[Union[str, Column]]] = None,
        where=None,
        limit: int = None,
        orderby=None,
    ):
        """
        Runs a 'select distinct' SQL query on the table.
        @fields specifies what fields to be unique over as 'all' or list(str).
        @where specifies a condition to meet.
        @limit specifies the maximum number of rows to return.
        @orderby specifies what to order by.
        """
        return self.db.query(
            "distinct",
            self.name,
            fields=fields,
            where=where,
            limit=limit,
            orderby=orderby,
        )

    def insert(self, fields: Mapping[Union[str, Column], Any]):
        """
        Runs an 'insert' SQL query on the table.
        @fields specifies what values to insert as Mapping(field : value)
        @extra is tacked on the end of the query.
        """
        return self.db.query("insert", self.name, fields=fields)

    def join(self, table, on: str, direction: str = "inner", alias: str = None):
        """
        Returns a Table of this table joined to @table.
        Returns None if there is an error.

        str @table is a Table object with .name and .columns{} properties.
        str @on specifies the joining condition.
        str @direction specifies 'inner', 'left', or 'right'; default 'inner'
        str @alias specifies the alias of @table.
        """
        if not table.db == self.db:
            raise ValueError("Table.join@table must be in the same database")
        if not isinstance(on, Column) or on.dtype != "comparison":
            raise TypeError('Table.join@on must be a Column with dtype "comparison"')
        if not direction.lower() in ["inner", "left", "right"]:
            raise ValueError("Table.join@direction must be 'inner', 'left', or 'right'")

        join = self.db.join(self, table, on=on, direction=direction, alias=alias)

        columns = [
            Column(c.table, repr(c), c.dtype) for c in self.columns + table.columns
        ]

        return Table(self.db, join, columns)

    def select(
        self, fields=None, where=None, limit: int = None, groupby=None, orderby=None
    ):
        """
        Runs a 'select' SQL query on the table.
        @fields specifies what fields to select as 'all' or list(str).
        @where specifies a condition to meet.
        @limit specifies the maximum number of rows to return.
        @groupby specifies what to group the data by
        @orderby specifies what to order by.
        """
        return self.db.query(
            "select",
            self.name,
            fields=fields,
            where=where,
            limit=limit,
            groupby=groupby,
            orderby=orderby,
        )

    def update(self, fields=None, where=None):
        """
        Runs an 'update' SQL query on the table.
        @fields specifies what values to update to as Mapping(field : value)
        @where specifies a condition to meet.
        """
        return self.db.query("update", self.name, fields=fields, where=where)
