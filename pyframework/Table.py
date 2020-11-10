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
	reqd : bool = True
		whether the column is required on not

	Properties
	----------
	dtype : Any
		the type of this column
	name : str
		the name of this column
	nullable : bool
		whether the column can take a null value
	reqd : bool
		whether this column is required
	table : Any
		the table to which this column belongs
	
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

	Class Methods
	-------------
	from_definition()
		creates a column from the given definition
	"""
	def __init__(self, table : Any, name: str, dtype : Any, reqd : bool = True) -> None:
		self._table = table
		self._name = name
		self._dtype = dtype
		self._reqd = reqd

	def __repr__(self) -> str:
		"""Makes a str represntation of self for queries"""
		return f"{self.table.name}.{self.name}" if hasattr(self.table, "name") else self.name

	def __str__(self) -> str:
		"""Makes a print-friendly str of self to display"""
		return f"{self.name} {self.dtype!s}"

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
	def name(self) -> str:
		"""Returns the name of the column"""
		return self._name
	@property
	def nullable(self) -> bool:
		"""
		Returns whether the column can take a null value
		Calculated as not self.reqd
		"""
		return not self.reqd
	@property
	def reqd(self) -> bool:
		"""Returns whether the column is required or not"""
		return self._reqd
	@property
	def table(self) -> Any:
		"""
		The Table this column is attached to
		Can be None if passed explicitly during creation
		"""
		return self._table

	def __bool__(self) -> bool:
		# handle comparisons by doing a comparison
		if self.dtype == "comparison":
			a, b = self.table
			
			# if both are Columns
			if isinstance(a, type(self)) and isinstance(b, type(self)):
				return all([a.table == b.table, a.name == b.name, a.dtype == b.dtype])

			# if one is Column and other is str
			elif (isinstance(a, type(self)) and isinstance(b, str)) or (isinstance(a, str) and isinstance(b, type(self))):
				# make b always the str
				if isinstance(a, str):
					a, b = b, a

				# handle table names
				if b.count(".") == 1:
					current = (a.table == b.split(".")[0]) and (a.name == b.split(".")[1].rsplit(" ",1)[0]) # rsplit in case of type
				else:
					current = (a.name == b)

				# handle type
				if b.count(" ") > 0:
					current = (current and a.dtype == b.rsplit(" ",1)[-1])

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


	@classmethod
	def from_definition(cls, definition) -> 'Column':
		"""Generator for a given defintion in the form '[table.]name[ with spaces] type' """
		name, dtype = definition.rsplit(" ", 1)
		if name.count('.') == 1:
			table, name = name.split(".")
		else:
			table = None
		return cls(table, name, dtype)
	
	def __add__(self, a):
		if hasattr(self.table, "db") and hasattr(self.self.table.db, "add"):
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
	columns : Iterable[Any]
		the column that belong to this table
		if column._table exists, sets it to the new table

	Properties
	----------
	columns : Iterable[Any]
		the column that belong to this table
		if column._table exists, sets it to the new table
	column_names : List[str]
		returns a list of the names of the columns
	db : Any
		the database to which this table belongs
	name : str
		the name of the table itself

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

	def __init__(self, db : Any, name : str, columns : Iterable[Any]) -> None:
		self._db = db
		self._name = name
		self._columns = columns

		for c in self.columns:
			if hasattr(c, "_table"):
				c._table = self

	def __repr__(self) -> str:
		return f"{self.name!r}" if 'join' not in self.name else self.name

	def __str__(self) -> str:
		"""Makes a str representation of self to display."""
		return f"Table {self.name}: " + ', '.join([str(c) for c in self.columns])

	@property
	def columns(self) -> Iterable[Any]:
		"""The columns of this table"""
		return self._columns
	@property
	def column_names(self) -> List[str]:
		"""A list of the column names"""
		return [i.name if hasattr(i, "name") else str(i) for i in self.columns]
	@property
	def db(self) -> Any:
		"""The db object given upon creation"""
		return self._db
	@property
	def name(self) -> str:
		"""The name of the table itself"""
		return self._name
	def get_column(self, col : Any) -> Any:
		"""
		Returns the given col if it's in self.columns or col : str in self.column_names
		Otherwise None
		"""
		if col in self.columns:
			print('self.columns', col, self.columns, col in self.columns)
			return col
		elif isinstance(col, str) and col in self.column_names:
			print('self.column_names')
			return self.columns[self.column_names.index(col)]
		elif isinstance(col, str) and self.name in col and col.replace(self.name, "").strip(".") in self.column_names:
			print('split self.column_names')
			return self.columns[self.column_names.index(col.replace(self.name, "").strip("."))]
		else:
			return None		

	def count(self, where : Column = None, groupby : Column = None, **kwargs):
		"""
		Runs a 'count' SQL query on the table.
		@where specifies a condition to meet.
		"""
		return self.db.query('count', self.name, where=where, groupby=groupby, **kwargs)

	def delete(self, where : Column = None):
		"""
		Runs a 'delete' SQL query on the table.
		@where specifies a condition to meet.
		"""
		return self.db.query('delete', self.name, where=where)

	def distinct(self, fields : Union[Iterable[Union[str, Column]]] = None, where = None, limit : int = None, orderby = None):
		"""
		Runs a 'select distinct' SQL query on the table.
		@fields specifies what fields to be unique over as 'all' or list(str).
		@where specifies a condition to meet.
		@limit specifies the maximum number of rows to return.
		@orderby specifies what to order by.
		"""
		return self.db.query('distinct', self.name, fields=fields, where=where, limit=limit, orderby=orderby)

	def insert(self, fields : Mapping[Union[str, Column], Any]):
		"""
		Runs an 'insert' SQL query on the table.
		@fields specifies what values to insert as Mapping(field : value)
		@extra is tacked on the end of the query.
		"""
		return self.db.query('insert', self.name, fields=fields)

	def join(self, table, on: str, direction: str = 'inner', alias: str = None):
		"""
		Returns a Table of this table joined to @table.
		Returns None if there is an error.

		str @table is a Table object with .name and .columns{} properties.
		str @on specifies the joining condition.
		str @direction specifies 'inner', 'left', or 'right'; default 'inner'
		str @alias specifies the alias of @table.
		"""
		if not table.db == self.db:
			raise ValueError('Table.join@table must be in the same database')
		if not isinstance(on, Column) or on.dtype != 'comparison':
			raise TypeError('Table.join@on must be a Column with dtype "comparison"')
		if not direction.lower() in ['inner', 'left', 'right']:
			raise ValueError("Table.join@direction must be 'inner', 'left', or 'right'")

		join = self.db.join(self, table, on=on, direction=direction, alias=alias)
		
		columns = [Column(c.table, repr(c), c.dtype) for c in self.columns + table.columns]

		return Table(self.db, join, columns)

	def select(self, fields = None, where = None, limit : int = None, groupby = None, orderby = None):
		"""
		Runs a 'select' SQL query on the table.
		@fields specifies what fields to select as 'all' or list(str).
		@where specifies a condition to meet.
		@limit specifies the maximum number of rows to return.
		@groupby specifies what to group the data by
		@orderby specifies what to order by.
		"""
		return self.db.query('select', self.name, fields=fields, where=where, limit=limit, groupby=groupby, orderby=orderby)

	def update(self, fields = None, where = None):
		"""
		Runs an 'update' SQL query on the table.
		@fields specifies what values to update to as Mapping(field : value)
		@where specifies a condition to meet.
		"""
		return self.db.query('update', self.name, fields=fields, where=where)