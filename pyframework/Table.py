import abc
from typing import Any, Iterable, List, Mapping, Union


class Column:
    """
    An object that represents a column within a table in a database and provides a simple interface

    Parameters
    ----------
    table : Table
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
    comment: str = ""
        a comment about the column

    Properties
    ----------
    table : Table
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
    comment: str = ""
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
        table: "Table",
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
        constraints: Iterable["AbstractConstraint"] = [],
    ) -> None:
        self._table = table
        self._name = name
        self._dtype = dtype
        self._null = True if null and not primary else False
        self._default = default
        self._visible = True if visible else False
        self._increment = True if increment else False
        self._comment = comment
        self._constraints = constraints

        if primary:
            PrimaryKey(self)
        elif unique:
            Unique(self)
        elif key:
            Index(self)

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
            kwargs["name"], definition = definition.split(
                None, 1
            )  # don't need to strip for None
        kwargs["name"] = kwargs["name"].lower()

        # don't know what dtype will look like
        # but we know what could come after it, so see what the first one of those is
        options = [
            "null",
            "default",
            "visible",
            "increment",
            "unique",
            "key",
            "comment",
        ]
        first_opt = options.pop(0)
        while first_opt and first_opt not in definition.lower():
            first_opt = options.pop(0) if len(options) else None

        # if there is one of them in definition
        if first_opt:
            # the dtype is before it
            dtype = definition[: definition.lower().index(first_opt)].strip()

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
                start = (
                    definition.index("default") + 1
                )  # one for name, one to get following
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
                kwargs["default"] = " ".join(definition[start:end])

            kwargs["visible"] = "visible" in definition or "invisible" not in definition
            kwargs["increment"] = (
                "auto_increment" in definition or "increment" in definition
            )
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
    def table(self) -> "Table":
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
        "comparison"
            the result of using comparison methods on two columns
        "operation"
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
        return self.get_constraint("unique") is not None

    @property
    def key(self) -> bool:
        """Returns whether or not this column is a key aka index"""
        return self.get_constraint("index") is not None

    @property
    def primary(self) -> bool:
        """
        Returns whether or not the column is the primary key of the table
        Implies both self.key and self.unique are True
        """
        return self.get_constraint("primary") is not None

    @property
    def comment(self) -> str:
        """Returns the comment on this column"""
        return self._comment

    @property
    def constraints(self) -> Iterable["AbstractConstraint"]:
        """Returns the constraints on this column"""
        return self._constraints

    def get_constraint(self, constraint: Union["AbstractConstraint", str]):
        """
        Returns the given constraint or None if it's in  ["unique", "primary", "index"]
            if multiple match, removes all but the returned instance
                a primary relation is kept in preference to unique
        Otherwise returns its argument
        """
        if isinstance(constraint, str) and constraint == "index":
            ret = [
                c for c in self.constraints if c.name in ["index", "primary", "unique"]
            ]

            if len(ret) == 0:
                return None
            elif len(ret) > 1:
                if any([c.name == "primary" for c in ret]):
                    # if the first one isn't the primary, we need to put it there
                    # this assures that primary is kept over any uniques or indexes
                    # False is less than True, so sort by the inverse
                    ret = sorted(ret, key=lambda x: x.name != "primary")
                elif any([c.name == "unique" for c in ret]):
                    # if there's no primary and the first one isn't the unique, we need to put it there
                    # this assures that unique is kept over any indexes
                    # False is less than True, so sort by the inverse
                    ret = sorted(ret, key=lambda x: x.name != "unique")

                # remove all of the repetitive ones except the first
                # see the conditional sorting above to determine the overall effect
                self._constraints = [
                    c
                    for c in self.constraints
                    if c.name not in ["index", "unique", "primary"]
                ] + ret[:1]
                return ret[0]

            else:  # len(ret) == 1
                return ret[0]

        elif isinstance(constraint, str) and constraint in ["unique", "primary"]:
            ret = [c for c in self.constraints if c.name in ["unique", "primary"]]

            if len(ret) == 0:
                return None
            elif len(ret) > 1:
                if (
                    any([c.name == "primary" for c in ret])
                    and not ret[0].name == "primary"
                ):
                    # if the first one isn't the primary, we need to put it there
                    # this assures that primary is kept over any uniques
                    # False is less than True, so sort by the inverse
                    ret = sorted(ret, key=lambda x: x.name != "primary")

                # remove all of the repetitive ones except the first
                # see the conditional sorting above to determine the overall effect
                self._constraints = [
                    c for c in self.constraints if c.name not in ["unique", "primary"]
                ] + ret[:1]
                return ret[0]
            else:  # len(ret) == 1
                return ret[0]
        elif isinstance(constraint, str) and any(
            [c.name == constraint for c in self.constraints]
        ):
            ret = [c for c in self.constraints if c.name == constraint]

            if len(ret) == 0:
                return None
            elif len(ret) > 1:
                # remove all of the repetitive ones except the first
                # asserting that same name means redundant
                # this can lead to unintended effects if different constraints have the same name
                self._constraints = [
                    c for c in self.constraints if c.name != constraint
                ] + ret[:1]
                return ret[0]
            else:  # len(ret) == 1
                return ret[0]

        else:
            return constraint

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
    comment: str = ""
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
    comment: str = ""
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

    @property
    def constraints(self) -> List["AbstractConstraint"]:
        """The constraints on all column of this table"""
        return [c for column in self.columns for c in column.constraints]

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

    def add_foreign_key(
        self, col: Union[str, Column], foreign: Union[str, Column], *, name: str = None
    ):
        """
        Runs an 'alter table add foreign key' SQL query on the table

        Parameters
        ----------
        col: str, Column
            the column on which to add the key
        foreign: str, Column
            the column to use as the foreign key
        *
        name: str = None
            the name to use for the foreign key
        """
        ForeignKey(col, foreign, name=name)
        return self.db.query("add foreign", self, col, foreign=foreign, name=name)

    def add_index(
        self, col: Union[str, Column], *, unique: bool = False, name: str = None
    ):
        """
        Runs an 'alter table add [unique] index' SQL query on the table for the given column

        Parameters
        ----------
        col: str, Column
            the column on which to add the key
        *
        unique: bool = False
            if True, make this a unique column
        name: str = None
            the name to use for the index
        """
        if unique:
            return self.add_unique(col, name=name)
        else:
            Index(col, name=name)
            return self.db.query("add index", self, col, name=name)

    def add_key(
        self,
        col: Union[str, Column],
        *,
        primary: bool = False,
        unique: bool = False,
        foreign: Union[str, Column] = None,
        name: str = None,
    ):
        """
        Runs an 'alter table add [primary / unique / foreign] key' SQL query on the table for the given column

        Parameters
        ----------
        col: str, Column
            the column on which to add the key
        *
        primary: bool = False
            if True, make this a primary key
            implies @unique = True as well
        unique: bool = False
            if True, make this a unique column
        foreign: str, Column = None
            if given, the column to use as the foreign key
            if @primary or @unique, calls them then returns result of foreign key
        name: str = None
            the name to use for the key
            if @foreign and (@primary or @unique), used for the foreign key
        """
        if foreign is not None:
            if primary or unique:
                self.add_key(col, primary=primary, unique=unique)
            return self.add_foreign_key(col, foreign, name=name)
        elif primary:
            return self.add_primary(col, name=name)
        elif unique:
            return self.add_unique(col, name=name)
        else:
            return self.add_index(col, name=name)

    def add_primary_key(self, col: Union[str, Column], *, name: str = None):
        """
        Runs an 'alter table add primary key' SQL query on the table

        Parameters
        ----------
        col: str, Column
            the column on which to add the key
        *
        name: str = None
            the name to use for the index
        """
        PrimaryKey(col, name=name)
        return self.db.query("add primary", self, col, name=name)

    def add_unique(self, col: Union[str, Column], *, name: str = None):
        """
        Runs an 'alter table add unique' SQL query on the table

        Parameters
        ----------
        col: str, Column
            the column on which to add the key
        *
        name: str = None
            the name to use for the index
        """
        Unique(col, name=name)
        return self.db.query("add unique", self, col, name=name)

    def count(
        self,
        where: Union[str, Column] = None,
        groupby: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Runs a 'count' SQL query on the table.

        Parameters
        ----------
        where : str, Column = None
            a condition that needs to be met to be selected
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        groupby : str, Column = None
            the field to group the data by
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query("count", self.name, where=where, groupby=groupby, **kwargs)

    def delete(self, where: Union[str, Column] = None, **kwargs):
        """
        Runs a 'delete' SQL query on the table.

        Parameters
        ----------
        where : str, Column = None
            a condition that needs to be met to be selected
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query("delete", self.name, where=where)

    def distinct(
        self,
        fields: Union[str, Column, Iterable[Union[str, Column]]] = "all",
        *,
        where: Union[str, Column] = None,
        limit: int = None,
        groupby: Union[str, Column] = None,
        orderby: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Runs a 'select distinct' SQL query on the table.

        Parameters
        ----------
        fields : str, Column, Iterable[str, Column], or "all"
            the distinct fields to select from the table
        *
        where : str, Column = None
            a condition that needs to be met to be selected
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        limit : int = None
            the maximum number of rows to return
        groupby : str, Column = None
            the field to group the data by
        orderby : str, Column = None
            the field to order the data by
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query(
            "distinct",
            self.name,
            fields=fields,
            where=where,
            limit=limit,
            groupby=groupby,
            orderby=orderby,
            **kwargs,
        )

    def drop_foreign_key(self, foreign_key: "ForeignKey" = False):
        """
        Runs an 'alter table drop foreign key' SQL query on the table

        Parameters
        ----------
        foreign_key: ForeignKey
            the foreign key to drop
        """
        ret = self.db.query("drop constraint", self, foreign_key)
        foreign_key.drop()
        return ret

    def drop_index(self, index: "Index", *, unique: bool = False):
        """
        Runs an 'alter table drop [unique] index' SQL query on the table for the given column

        Parameters
        ----------
        index: Index
            the index to drop
        *
        unique: bool = False
            whether the key to drop is a unique key
        """
        if unique:
            return self.drop_unique(index)
        else:
            ret = self.db.query("drop constraint", self, index)
            index.drop()
            return ret

    def drop_key(
        self,
        key: "AbstractConstraint",
        *,
        primary: bool = False,
        unique: bool = False,
        foreign: Union[bool, str, Column] = False,
    ):
        """
        Runs an 'alter table drop [primary / unique / foreign] key' SQL query on the table for the given column

        Parameters
        ----------
        key: AbstractConstraint
            the key to drop
        *
        primary: bool = False
            whether the key to drop is a primary key
            if True, implies @unique = True as well
        unique: bool = False
            whether the key to drop is a unique key
        foreign: bool, str, Column = False
            if bool, whether the key to drop is a foreign key
            if not bool, the column the foreign key references
            if @primary or @unique, calls them then returns result of foreign key
        """
        if foreign:
            if primary or unique:
                self.drop_key(key, primary=primary, unique=unique)
            return self.drop_foreign_key(key)
        elif primary:
            return self.drop_primary(key)
        elif unique:
            return self.drop_unique(key)
        else:
            return self.drop_index(key)

    def drop_primary_key(self, primary_key: "PrimaryKey"):
        """
        Runs an 'alter table drop primary key' SQL query on the table

        Parameters
        ----------
        primary_key: PrimaryKey
            the primary key to drop
        """
        ret = self.db.query("drop constraint", self, primary_key)
        primary_key.drop()
        return ret

    def drop_unique(self, unique: "Unique"):
        """
        Runs an 'alter table drop unique' SQL query on the table

        Parameters
        ----------
        unique: Unique
            the unique constraint to drop
        """
        ret = self.db.query("drop constraint", self, unique)
        unique.drop()
        return ret

    def insert(self, fields: Mapping[Union[str, Column], Any], **kwargs):
        """
        Runs an 'insert' SQL query on the table.

        Parameters
        ----------
        fields : Mapping[Union[str, Column] : Any]
            what values to insert into the table
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query("insert", self.name, fields=fields, **kwargs)

    def join(
        self, table: "Table", on: str, direction: str = "inner", alias: str = None
    ):
        """
        Returns a Table of this table joined to @table.
        Returns None if there is an error.

        str @table is a Table object with .name and .columns{} properties.
        str @on specifies the joining condition.
        str @direction specifies 'inner', 'left', or 'right'; default "inner"
        str @alias specifies the alias of @table.
        """
        if not table.db == self.db:
            raise ValueError("Table.join@table must be in the same database")
        if not isinstance(on, Column) or on.dtype != "comparison":
            raise TypeError('Table.join@on must be a Column with dtype "comparison"')
        if not direction.lower() in ["inner", "left", "right"]:
            raise ValueError('Table.join@direction must be "inner", "left", or "right"')

        join = self.db.join(self, table, on=on, direction=direction, alias=alias)

        columns = [
            Column(c.table, repr(c), c.dtype) for c in self.columns + table.columns
        ]

        return Table(self.db, join, columns)

    def select(
        self,
        fields: Union[str, Column, Iterable[Union[str, Column]]] = "all",
        *,
        where: Union[str, Column] = None,
        limit: int = None,
        groupby: Union[str, Column] = None,
        orderby: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Runs a 'select' SQL query on the table.

        Parameters
        ----------
        fields : str, Column, Iterable[str, Column], or "all"
            the fields to select from the table
        *
        where : str, Column = None
            a condition that needs to be met to be selected
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        limit : int = None
            the maximum number of rows to return
        groupby : str, Column = None
            the field to group the data by
        orderby : str, Column = None
            the field to order the data by
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query(
            "select",
            self.name,
            fields=fields,
            where=where,
            limit=limit,
            groupby=groupby,
            orderby=orderby,
            **kwargs,
        )

    def update(
        self,
        fields: Mapping[Union[str, Column], Any] = None,
        where: Union[str, Column] = None,
        **kwargs,
    ):
        """
        Runs an 'update' SQL query on the table.

        Parameters
        ----------
        fields :  Mapping[Union[str,column], Any)
            the values to update in the table
        where : str, Column = None
            a condition that needs to be met to be selected
            Column with dtype in 'comparison', generated from performing a comparison on a Column
        **kwargs
            passed to self.db.query, and onwards to self.db.[prepare / translate / db_query / interpret]
        """
        return self.db.query("update", self.name, fields=fields, where=where, **kwargs)


class AbstractConstraint(abc.ABC):
    """
    An object that represents a constraint in the database

    Parameters
    ----------
    target : Column
        the column to add this constraint to
        appends itself to target.constraints
    *

    name : str
        the name of this constraint

    Properties
    ----------
    target : Column
        the given target column
    name : str
        the name of this constraint
        either given on __init__ or set by child class

    Methods
    -------
    *validate()
        returns self unless a given value would be invalid to insert
    """

    def __init__(self, target: Column, *, name: str = ""):
        self._target = target
        self._target._constraints.append(self)
        if name:
            self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def target(self) -> Column:
        return self._target

    @abc.abstractmethod
    def validate(self, value: Any) -> "AbstractConstraint":
        """
        Provide some method of checking if a given value would be valid to insert under this constraint

        Should return self or raise a ValueError
        """
        pass

    def drop(self):
        self._target._constraints.pop(self._target._constraints.index(self))


class Index(AbstractConstraint):
    _name: str = "index"

    def prepare(self) -> "Index":
        all_keys = [
            column.get_constraint("index") for column in self.target.table.columns
        ]
        results = self.target.table.select(
            [c.target for c in all_keys if c is not None]
        )

        if not isinstance(results, Iterable) and all(
            [isinstance(r, Mapping) and self.target.name in r for r in results]
        ):
            raise NotImplementedError(
                f"Index.prepare is only implemented when self.target.table.select returns Iterable[Mapping(self.target.name: <key>, ...)]\nPlease reimplement Index.prepare to handle the return: {ret}"
            )

        self.values = {}
        for r in results:
            key = r.pop(self.target.name)
            self.values[key] = r

    def validate(self, value: Any) -> "Index":
        if not hasattr(self, "values"):
            self.prepare()
        return self


class Unique(AbstractConstraint):
    _name: str = "unique"

    def prepare(self) -> "Unique":
        results = self.target.table.distinct(self.target)
        if not isinstance(results, Iterable) and all(
            [isinstance(r, Mapping) and self.target.name in r for r in results]
        ):
            raise NotImplementedError(
                f"Index.prepare is only implemented when self.target.table.select returns Iterable[Mapping(self.target.name: <key>, ...)]\nPlease reimplement Index.prepare to handle the return: {ret}"
            )
        self.values = [r[self.target.name] for r in results]
        return self

    def validate(self, value: Any) -> "Unique":
        if not hasattr(self, "values"):
            self.prepare()
        if value in self.values:
            raise ValueError(f"{value} is not unique in {self.target}")
        return self


class PrimaryKey(Unique):
    _name: str = "primary"

    def __init__(self, target: Column):
        if target.null:
            raise ValueError(
                f"A primary key may only be created if the target column is NOT NULL. target: {target}"
            )
        super().__init__(target)


class ForeignKey(AbstractConstraint):
    def __init__(self, target: Column, foreign: Column, name: str = ""):
        if target.table == foreign.table:
            raise ValueError(
                f"Cannot foreign key a table on itself\ntarget: {target}\nforeign: {foreign}"
            )

        super().__init__(target, name=name)

        self._foreign = foreign
        self._foreign._constraints.append(self)

        if not name or name == "":
            self._name = (
                f"fk_{self.target.table}_{self.target.name}_{self.foreign.name}"
            )

        if not self.foreign.get_constraint("index"):
            Index(foreign)

    @property
    def foreign(self) -> Column:
        return self._foreign

    def prepare(self) -> "ForeignKey":
        index = self.foreign.get_constraint("index")
        index.prepare()
        self.values = list(index.values.keys())

    def validate(self, value: Any) -> "ForeignKey":
        if not hasattr(self, "values"):
            self.prepare()
        return value in self.values
