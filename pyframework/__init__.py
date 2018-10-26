"""
A wrapper model of MySQLdb that implements Entity Framework-like behavior using object-oriented programming.
Can be extended to any other type of database by overwriting method `sql_query`.

Requirements:
	MySQLdb
"""

import gc
import MySQLdb
from MySQLdb.converters import conversions as CONVERSIONS
import os
from typing import Any, Dict, Iterable, List, Union

escape_string = MySQLdb.escape_string



class Table:
	"""
An object that represents a table in a MySQL database and provides a simple interface.
One Table is made for each table in the Database.
All data-returning methods call the db's `query` method and pass it all kwargs.

Properties:
	cols (List[Dict[str, str]]) - returns a list of dicts describing the columns
	name (str) - returns the name of the table itself
	col_names (List[str]) - returns a list of the names of the columns

Methods:
	count(where, **kwargs) - counts the entries that meet the criteria in @where
	delete(where, **kwargs) - deletes the entries which meet the criteria in @where
	distinct(fields, where, limit, orderby, **kwargs) - selects entries with distinct @fields
		that meet the criteria in @where ordered by @orderby; only returns @limit entries max
	insert(fields, extra, **kwargs) - inserts the values in @fields
	select(fields, where, limit, groupby, orderby, **kwargs) - selects the specified @fields
		for entries that meet the criteria in @where grouped by @groupby and ordered by @orderby;
		only returns @limit entries max
	update(fields, where, **kwargs) - updates the given @fields for entries that meet the
		criteria in @where
	join(tbl, on, alias, direction) - returns a Table that allows for querying on the inner/left/
		right joined with @tbl aliased as @alias on @on

	"""

	def __init__(self, db, name: str, cols: list):
		self._db = db
		self._name = name
		self._cols = cols

	def __repr__(self):
		"""
Makes a str representation of self to display.
		"""
		return 'Table ' + self.name  + ': ' + ', '.join([i['Type'] + ' ' + i['Field'] for i in self._cols])

	@property
	def cols(self):
		"""
Returns a dict of column descriptions.
		"""
		return self._cols

	@property
	def name(self):
		"""
Returns the name of the table itself.
		"""
		return self._name
	
	@property
	def col_names(self):
		"""
Returns a list of the column names.
		"""
		return [i['Field'] for i in self._cols]

	def count(self, where: str = None, **kwargs):
		"""
Runs a 'count' SQL query on the table.
@where specifies a condition to meet.
		"""
		where = 'WHERE ' + where if where else ''
		return self._db.query('count', self._name, extra=where, **kwargs)

	def delete(self, where: str = None, **kwargs):
		"""
Runs a 'delete' SQL query on the table.
@where specifies a condition to meet.
		"""
		where = 'WHERE ' + where if where else ''
		return self._db.query('delete', self._name, extra=where, **kwargs)

	def distinct(self, fields = None, where: str = None, limit: int = None, orderby: str = None, **kwargs):
		"""
Runs a 'select distinct' SQL query on the table.
@fields specifies what fields to be unique over as 'all' or list(str).
@where specifies a condition to meet.
@limit specifies the maximum number of rows to return.
@orderby specifies what to order by.
		"""
		where = 'WHERE ' + where 		if where else ''
		limit = 'LIMIT ' + str(limit)	if limit else ''
		orderby = 'ORDER BY ' + orderby	if orderby else ''
		return self._db.query('distinct', self._name, fields, where, **kwargs)

	def insert(self, fields = None, extra: str = None, **kwargs):
		"""
Runs an 'insert' SQL query on the table.
@fields specifies what values to insert as dict(field : value)
@extra is tacked on the end of the query.
		"""
		return self._db.query('insert', self._name, fields, extra, **kwargs)

	def select(self, fields = None, where: str = None, limit: int = None, groupby: str = None, orderby: str = None, **kwargs):
		"""
Runs a 'select' SQL query on the table.
@fields specifies what fields to select as 'all' or list(str).
@where specifies a condition to meet.
@limit specifies the maximum number of rows to return.
@groupby specifies what to group the data by
@orderby specifies what to order by.
		"""
		where 	= 'WHERE ' + where 		if where else ''
		limit 	= 'LIMIT ' + str(limit) if limit else ''
		groupby = 'GROUP BY ' + groupby if groupby else ''
		orderby = 'ORDER BY ' + orderby	if orderby else ''
		return self._db.query('select', self._name, fields, ' '.join([where, limit, orderby, groupby]), **kwargs)

	def update(self, fields = None, where: str = None, **kwargs):
		"""
Runs an 'update' SQL query on the table.
@fields specifies what values to update to as dict(field : value)
@where specifies a condition to meet.
		"""
		where = 'WHERE ' + where if where else ''
		return self._db.query('update', self._name, fields, where, **kwargs)

	def join(self, tbl, on: str, alias: str = None, direction: str = 'inner'):
		"""
Returns a Table of this table joined to @tbl.
Returns None if there is an error.

str @tbl is a Table object with .name and .cols{} properties.
str @on specifies the joining condition.
str @alias specifies the alias of @tbl.
str @direction specifies 'inner', 'left', or 'right'; default 'inner'
		"""
		if not direction.lower() in ['inner', 'left', 'right']: return None

		join = self.name + ' ' + direction.upper() + ' JOIN ' + tbl.name + ('AS ' + alias if alias else '') + ' ON ' + on
		
		cols = self.cols
		for i in range(len(cols)):
			cols[i]['Field'] = self.name + '.' + cols[i]['Field']

		temp = tbl.cols
		for i in range(len(temp)):
			temp[i]['Field'] = tbl.name + '.' + temp[i]['Field']

		cols += temp

		return Table(self._db, join, cols)



class Database:
	"""
An object that represents a MySQL database and allows for interaction with it.

str @loc is location of the MySQL server
str @user is the MySQL username
str @passwd is the password for @user
str @db is the name of the db

Properties:
	open (bool) - whether or not the database connection is open
	tables (List[Table]) - a Table object for each table in the database
	table_names (List[str]) - a list of the names of all the tables

Methods:
	connect() - connects to the database
	commit() - commits changes to the database
	rollback() - rolls back changes to the database
	close() - closes the connection to the database
	reconnect() - same as calling close() then open()
	sql_query(q, limit, offset, kwargs) - queries the database for q, only returns @limit rows starting at @offset; kwargs passed to MySQLdb's fetch_row
	query(method, table, fields, extra, kwargs) - builds a SQL query `q` and calls sql_query(q, **kwargs)
	make_classes() - produces a class for each table
	remake_class(name) - remakes the class for the given table
	make_table(name, fields, temp, clobber, mk_class) - makes a new table with the given name and fields; clobber to overwrite existing table
	drop_table(name, temp, rm_class) - drop the given table; temp to only drop TEMPORARY tables
	move_table(old_name, new_name, mv_class) - renames the given table to the given name
	truncate_table(name) - truncates the given table
	add_column(name, field, definition, alter_class) - add the given column to the given table
	drop_column(name, field, alter_class) - drop the given column from the given table
	alter_column(name, field, definition, new_name, alter_class) - alter the given column in the given table
	add_fk(name, table, field, ref) - creates a foreign key between table(field) and ref
	drop_fk(name, table) - drops the named foreign key on the given table
	"""
	def __init__(self, loc, user, passwd, db):
		self._location	= loc
		self._user		= user
		self._password	= passwd
		self._database	= db
		self._db 		= None
		self._tables 	= None

	@property
	def open(self) -> bool:
		"""
Returns whether or not the db connection is open.
		"""
		return (not self._db is None) and bool(self._db.open)

	@property
	def tables(self) -> list:
		"""
Returns the list of Table objects for the db,
	or None if not connected.
		"""
		return self._tables

	@property
	def table_names(self) -> list:
		"""
Returns a list of the names of the Table objects in the db
		"""
		return [i.name for i in self._tables] if self._tables else []
	

	def connect(self):
		"""
Opens a new connection to the db.
Sets up the self._tables as List[Table]
If a connection is already open, does nothing.
		"""
		if self.open: return self

		# initiate the connection
		self._db = MySQLdb.connect(self._location, self._user, self._password,
			self._database, use_unicode=True, charset='utf8mb4')

		# set up the tables
		tbls = self.sql_query('SHOW TABLES;')
		if tbls:
			tbls = [list(i.values())[0] for i in tbls]
			for i in tbls:
				cols = self.sql_query('DESCRIBE ' + i + ';')
				setattr(self, i, Table(self, i, cols))

			self._tables = [getattr(self, i) for i in tbls]

		return self

	def commit(self):
		"""
Commits changes to the db.
Does nothing if the db is not open.
		"""
		if self.open:
			self._db.commit()
		return self

	def rollback(self):
		"""
Rolls back any changes to the db.
Does nothing if the db is not open.
		"""
		if self.open:
			self._db.rollback()
		return self

	def close(self):
		"""
Closes the db if it's open.
Does nothing if the db is not open.
		"""
		if self.open:
			self._tables = None
			tbls = self.sql_query('SHOW TABLES;')
			tbls = [list(i.values())[0] for i in tbls]
			for i in tbls:
				delattr(self,i)

			self._db.close()
			self._db = None
			gc.collect()
		return self

	def reconnect(self):
		"""
Makes sure the db is open.
If the db is already open, it closes and connects again.
		"""
		self.close()
		return self.connect()


	def sql_query(self, q: str, limit: int = -1, offset: int = 0, **kwargs) -> List[Dict[str, Any]]:
		"""
Returns the results of the query as a list of dictionaries unless @how is specified.
If no result, such as UPDATE, DELETE, etc, returns None.
Can raise a MySQLdb.MySQLError if a exception occurs.

str @q is the MySQL query, no validation is done for correctness. Must end in ';'.
int @limit is the number of results to return. @limit < 0 for all, default -1.
int @offset is the number of rows to skip at the beginning. Only used if @limit is set.

kwargs passed to fetch_row. See MySQLdb documentation:
	int @maxrows > 0
	int @how in [0,1,2]
		"""

		# checks
		if not self.open:
			raise Exception('You must initiate the connection.')
		if not type(q) is str:
			raise TypeError('You must pass Database.query a str.')
		if not type(limit) is int:
			raise TypeError('Database.query@limit must be a positive int.')
		if not type(offset) is int:
			raise TypeError('Database.query@offset must be a positive int.')

		# append `limit` statement
		if limit > 0:
			q = q[:-1]
			q += ' LIMIT '
			if offset > 0:
				q += str(offset) + ', '
			q += str(limit) + ';'

		# run the call
		self._db.query(q)
		r = self._db.store_result()
		
		# return None if no results
		if r is None:
			return r
	
		# deal with maxrows
		maxrows = 0 # default to all
		if 'maxrows' in kwargs:
			maxrows = kwargs['maxrows']
			if not type(maxrows) is int:
				raise TypeError('Database.sql_query@maxrows must be an int.')
			elif maxrows < 0:
				raise ValueError('Database.sql_query@maxrows must be positibe')
			del kwargs['maxrows']

		# deal with `how` kwarg
		how = 1 # default to dictionary
		if 'how' in kwargs:
			how = kwargs['how']
			if not type(how) is int:
				'Database.sql_query@how must be an int.'
			elif not 0 <= how <= 2:
				'Database.sql_query@how must be 0, 1, or 2.'
			del kwargs['how']

		# get the results
		ret = list(r.fetch_row(maxrows=maxrows, how=how))

		# handle MySQL `bytes` type and convert to str or int
		if ret and len(ret) > 0 and type(ret[0]) is dict: # if is List[dict]
			ret = [{
						k: (
							(
								int(str(v)[4:-1],16) if '\\x' in str(v) # read as hexadecimal if b'\xXX'
													 else str(v)[2:-1] # return it as str otherwise
							) if type(v) is bytes
							  else  v # if it's not a `bytes`, do nothing
						) for k,v in i.items()
					} for i in ret]


		# if returned one dict, just return as bare dict
		if ret and len(ret) == 1:
			ret = ret[0]
			# if returned just one columns, return as bare object
			if len(ret) == 1:
				ret = list(ret.values())[0]

		return ret

	def query(self, method: str, tbl: str, fields: Union[None, Dict[str, Any], List[str], str] = None, extra: str = None, **kwargs):
		"""
str @method is the MySQL method argument. Use 'distinct' for `select distinct`
str @tbl is the table name
@fields
	Not required for method = 'count', 'delete'
	Must be a dict of {field: value} for method = 'insert', 'update'
	Must be a list(str) or str or 'all' otherwise.
str @extra is tacked on the end of the MySQL query. This includes `where`, `order by`, etc
		"""
		# checks
		if not self.open:
			raise Exception('You must initiate the connection.')

		if not type(method) is str:
			raise TypeError('Database.query@method must be a str.')
		method = method.lower()

		if not method in ['distinct','select','insert','delete','update','count']:
			raise ValueError('Database.query can only select, insert, delete, count, or update.')
		if not type(tbl) is str:
			raise TypeError('Database.query@tbl must be a str.')

		if not method in ['count','delete']:
			if method in ['insert','update']:
				if not type(fields) is dict:
					raise TypeError('Database.query@fields must be a dict for insert. \'all\' is not accepted.')
			elif type(fields) is list:
				for i in fields:
					if not type(i) is str:
						raise TypeError('Database.query@fields must be \'all\' or a list of str.')
			else:
				if not type(fields) is str or not fields == 'all':
					raise TypeError('Database.query@fields must be \'all\' or a list of str.')

		if not extra is None:
			if not type(extra) is str:
				raise TypeError('Database.query@extra must be a str.')
			if ';' in extra:
				raise Exception('Invalid MySQL command')

		# build the query
		sql = method.upper() + ' '
		if method in ['select', 'count', 'distinct','delete']:
			if method in ['select','distinct']:
				if method == 'distinct':
					sql = 'select distinct '.upper()

				if fields == 'all':
					sql += '*'
				else:
					for i in fields:
						sql += i + ','
					sql = sql[:-1]
			elif method == 'count':
				sql = 'select COUNT(*)'.upper()
			sql += ' FROM ' + tbl

		elif method == 'insert':
			sql += 'INTO ' + tbl + '('
			for i in fields:
				sql += i + ','
			sql = sql[:-1]+')'
			sql += ' VALUES('
			for i in fields.values():
				sql += '\'' + MySQLdb.escape_string(i).decode() + '\','
			sql = sql[:-1]+')'

		elif method == 'update':
			sql += tbl + ' SET '
			for f,v in fields.items():
				sql += f + '=\'' + MySQLdb.escape_string(v).decode() + '\','
			sql= sql[:-1]

		# append the extra clauses
		if extra:
			sql += ' ' + extra
		sql += ';'

		# call sql_query to get results
		return self.sql_query(sql, **kwargs)


	def make_classes(self):
		"""
Call to generate a class for each table in db.
If the db is closed, temporarily opens a connection.
Classes are put into the `tables` directory.

Not yet utilized anywhere.
		"""
		# open the connection if needed
		# keep track of if we need to close it
		if not self.open:
			self.connect()
			close = True
		else: close = False

		# make directory `tables` if needed
		if not os.path.isdir('tables'):
			os.mkdir('tables')

		# get the tables
		tbls = self.sql_query('SHOW TABLES;')
		tbls = [list(i.values())[0] for i in tbls]

		# write init file for tables module
		with open('tables/__init__.py','w') as f:
			for i in tbls:
				f.write('from tables.' + i + ' import *\n')

		# write the classes
		for i in tbls:
			cols = self.sql_query('DESCRIBE ' + i + ';')
			with open('tables/' + i + '.py','w') as f:
				f.write('class ' + i + ':\n')
				f.write('\tdef __init__(self, data):\n')
				for j in cols:
					f.write('\t\tself.' + j['Field'] + ' = data[\'' + j['Field'] + '\']\n')

				f.write('\t\tself.cols = [' + ', '.join(['\'' + j['Field'] + '\'' for j in cols]) + ']\n')

				f.write('\n\tdef __repr__(self):\n')
				f.write('\t\treturn str({i: getattr(self,i) for i in self.cols})')

		# close if needed
		if close:
			self.close()
		return self


	def remake_class(self, name: str):
		"""
Remakes the class for a specific table.
Does nothing if is not open.

str @name is the table for which to reload the table
		"""
		if not self.open:
			return self

		cols = self.sql_query('DESCRIBE ' + name + ';')

		if not os.path.isdir('tables'):
			os.mkdir('tables')
		with open('tables/' + name + '.py','w') as f:
			f.write('class ' + name + ':\n')
			f.write('\tdef __init__(self, data):\n')
			for j in cols:
				f.write('\t\tself.' + j['Field'] + ' = data[\'' + j['Field'] + '\']\n')

			f.write('\t\tself.cols = [' + ', '.join(['\'' + j['Field'] + '\'' for j in cols]) + ']\n')

			f.write('\n\tdef __repr__(self):\n')
		
		return self


	def make_table(self, name: str, fields: Union[str, Dict[str, str], Iterable[str]], temp: bool = False, clobber: bool = False, mk_class: bool = True) -> Union[Table, None]:
		"""
Creates a new table in the database and generates its class.
Returns the Table object for the new table.
Returns None if the db is not open.

str @name is the name of the table
@fields can be:
	str as comma-separated column definitions;
	Iterable[str] as columns definitions; or
	Dict[str, str] as {field: definition}
bool @temp to make a temporary table
bool @clobber to overwrite an existing table; default False
bool @mk_class to create the class definition in dir `tables`; default True
		"""
		# checks
		if not self.open:
			return None

		# create the query
		sql = 'CREATE '
		if temp: sql += 'TEMPORARY '
		sql += 'TABLE '
		if not clobber: sql += 'IF NOT EXISTS '
		sql += name + '('

		if type(fields) is str:
			sql += fields
		elif not type(fields) is dict:
			sql += ', '.join(list(fields))
		else: # is a dict
			sql += ', '.join([k + ' ' + v for k,v in fields.items()])
				
		sql += ');'

		# create the table
		self.sql_query(sql)
		cols = self.sql_query('DESCRIBE ' + name + ';')

		# make the class if needed
		if mk_class:
			if not os.path.isdir('tables'):
				os.mkdir('tables')
			with open('tables/' + name + '.py','w') as f:
				f.write('class ' + name + ':\n')
				f.write('\tdef __init__(self, data):\n')
				for j in cols:
					f.write('\t\tself.' + j['Field'] + ' = data[\'' + j['Field'] + '\']\n')

				f.write('\t\tself.cols = [' + ', '.join(['\'' + j['Field'] + '\'' for j in cols]) + ']\n')

				f.write('\n\tdef __repr__(self):\n')
				f.write('\t\treturn str({i: getattr(self,i) for i in self.cols})')

		# make the Table and return it
		setattr(self, name, Table(self, name, cols))
		if name in self.table_names:
			del self._tables[self.table_names.index(name)]
		self._tables += [getattr(self, name)]
		return getattr(self, name)


	def drop_table(self, name: str, temp: bool = False, rm_class: bool = True):
		"""
Drops the given table.
Requires DROP permission for the user.
Does nothing if not open.

str @name is the table to drop
bool @temp to specify only drop TEMPORARY tables; default False
bool @rm_class to also remove the class from `tables`; default True
		"""
		if not self.open:
			return self

		sql = 'DROP '
		if temp: sql += 'TEMPORARY '
		sql += 'TABLE IF EXISTS ' + name + ';'
		self.sql_query(sql)

		# delete the class if needed
		if rm_class and os.path.isdir('tables'):
			if os.path.exists('tables/'+name+'.py'):
				os.remove('tables/'+name+'.py')

		# remove the table from self
		if name in self.table_names:
			del self._tables[self.table_names.index(name)]
		if hasattr(self, name):
			delattr(self, name)
		return self


	def move_table(self, old_name: str, new_name: str, mv_class: bool = True):
		"""
Renames the given table with the new name.
Returns the new Table object.
Returns None if not open.

str @old_name is the current name of the table
str @new_name is the new name to call the table
bool @mv_class to also rename the class in `tables`; default True
		"""
		if not self.open:
			return None

		sql = 'ALTER TABLE ' + old_name + ' RENAME TO ' + new_name + ';'
		self.sql_query(sql)
		cols = self.sql_query('DESCRIBE ' + new_name + ';')

		# delete and remake the class if necessary
		if mv_class:
			if not os.path.isdir('tables'): os.mkdir('tables')
			elif os.path.exists('tables/'+old_name+'.py'): os.remove('tables/'+old_name+'.py')

			with open('tables/' + new_name + '.py','w') as f:
				f.write('class ' + new_name + ':\n')
				f.write('\tdef __init__(self, data):\n')
				for j in cols:
					f.write('\t\tself.' + j['Field'] + ' = data[\'' + j['Field'] + '\']\n')

				f.write('\t\tself.cols = [' + ', '.join(['\'' + j['Field'] + '\'' for j in cols]) + ']\n')

				f.write('\n\tdef __repr__(self):\n')
				f.write('\t\treturn str({i: getattr(self,i) for i in self.cols})')

		# make the Table and return it
		if hasattr(self, old_name): delattr(self, old_name)
		setattr(self, new_name, Table(self, new_name, cols))
		if old_name in self.table_names: del self._tables[self.table_names.index(old_name)]
		self._tables += [getattr(self, new_name)]
		return getattr(self, new_name)


	def truncate_table(self, name):
		"""
Truncates the given table.
		"""
		if self.open: self.sql_query('TRUNCATE TABLE ' + name + ';')
		return self

	def add_column(self, name: str, field: str, definition: str, after: str, alter_class: bool = True):
		"""
Adds a column to an existing table.
Returns the Table object.
Returns None if is not open.

str @name is the name of the table to alter
str @field is the name of the column to add
str @definition is the definition of the column
str @after is the column after which to add the new column; 'first' also allowed
bool @alter_class to also remake the class in `tables`; default True
		"""
		if not self.open: return None

		sql = 'ALTER TABLE ' + name + ' ADD COLUMN ' + field + ' ' + definition

		if after:
			if after.lower() == 'first': sql += ' FIRST'
			else: sql += ' AFTER ' + after
		sql += ';'

		self.sql_query(sql)

		if alter_class:
			self.remake_class(name)

		cols = self.sql_query('DESCRIBE ' + name + ';')
		if hasattr(self, name): delattr(self, name)
		setattr(self, name, Table(self, name, cols))
		if name in self.table_names: del self._tables[self.table_names.index(name)]
		self._tables += [getattr(self, name)]
		return getattr(self, name)

	def drop_column(self, name: str, field: str, alter_class: bool = True):
		"""
Drops the given column from the given table.
Does not check if dropping the column will be a problem.
Returns the new Table object.
Returns None if is not open.

str @name is the name of the table to alter
str @field is the name of the column to drop
bool @alter_class to also remake the class in `tables`; default True
		"""
		if not self.open: return None

		sql = 'ALTER TABLE ' + name + ' DROP COLUMN '+ field + ';'
		self.sql_query(sql)

		if alter_class: self.remake_class(name)

		cols = self.sql_query('DESCRIBE ' + name + ';')
		if hasattr(self, name): delattr(self, name)
		setattr(self, name, Table(self, name, cols))
		if name in self.table_names: del self._tables[self.table_names.index(name)]
		self._tables += [getattr(self, name)]
		return getattr(self, name)


	def alter_column(self, name: str, field: str, definition: str, new_name: str = '', alter_class: bool = True):
		"""
Alters the given column in the given table.
Returns the new Table object; or None if is not open.

str @name is the name of the table
str @field is the name of the column
str @definition is the new definition of the column
str @new_name is the new name of the column; default '' to not rename
bool @alter_class to also remake the class in `tables`; default True
		"""
		if not self.open: return None

		sql = 'ALTER TABLE ' + name + ' CHANGE COLUMN ' + field + ' '
		sql += new_name if new_name else name
		sql += ' ' + definition + ';'
		self.sql_query(sql)

		if alter_class: self.remake_class(name)

		cols = self.sql_query('DESCRIBE ' + name + ';')
		if hasattr(self, name): delattr(self, name)
		setattr(self, name, Table(self, name, cols))
		if name in self.table_names: del self._tables[self.table_names.index(name)]
		self._tables += [getattr(self, name)]
		return getattr(self, name)

	def add_fk(self, name: str, table: str, field: str, ref: str):
		"""
Adds a foreign key constraint.
str @name is the name of the fk
str @table is the table
str @field is the column
str @ref is the table and column that is the key; in the form `tbl`(`col`)
		"""
		if self.open:
			sql = 'ALTER TABLE ' + table + ' ADD CONSTRAINT fk_' + name
			sql += ' FOREIGN KEY (' + field + ') REFERENCES ' + ref + ';'
			self.sql_query(sql)
		return self

	def drop_fk(self, name: str, table: str):
		"""
Drop a foreign key constraint.
str @name is the name of the fk
str @table is the table
		"""
		if self.open:
			sql = 'ALTER TABLE ' + table + ' DROP FOREIGN KEY fk_' + name + ';'
			self.sql_query(sql)
		return self



MYSQL_TYPES = {
	'BIT[(M)]': 'an M bit number; default M=1',
	'TINYINT[(M)] [UNSIGNED]': 'a number in the range [-128,127]; or [0,255] if unsigned',
	'BOOL': 'same as TINYINT(1); 0 is FALSE, non-zero is TRUE',
	'SMALLINT[(M)] [UNSIGNED]': 'a number in the range [-32768,32767]; or [0,65535] if unsigned',
	'MEDIUMINT[(M)] [UNSIGNED]': 'a number in the range [-8388608,8388607]; or [0,16777215] if unsigned',
	'INT[(M)] [UNSIGNED]': 'a number in the range [-2147483648,2147483647]; or [0,4294967295] if unsigned',
	'BIGINT[(M)] [UNSIGNED]': 'a number in the range [-9223372036854775808,9223372036854775807]; or [0,18446744073709551615] if unsigned',
	'SERIAL'	: 'same as BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE',
	'DECIMAL[(M[,D])] [UNSIGNED]': 'an M digit number with D digits after the decimal point; default M=10, D=0; max M=65, D=30',
	'FLOAT[(M,D)] [UNSIGNED]': 'an M digit number with D digits after the decimal point; default M,D = max; accurate to ~7 decimal places',
	'DOUBLE[(M,D)] [UNSIGNED]': 'an M digit number with D digits after the decimal point; default M,D = max; accurate to ~15 decimal places',
	'DATE': 'generic date in the range 1000-01-01 to 9999-12-31',
	'DATETIME[(fsp)]': 'a datetime in the range 1000-01-01 00:00:00.000000 to 9999-12-31 23:59:59.999999; fsp is the precision of fractional seconds, default = 0',
	'TIMESTAMP[(fsp)]': 'a UTC timestamp in the range 1970-01-01 00:00:01.000000 to 2038-01-19 03:14:07.999999; fsp is the precision of fractional seconds, default = 0',
	'TIME[(fsp)]': 'a time in the range -838:59:59.000000 to 838:59:59.000000; fsp is the precision of fractional seconds, default = 0',
	'YEAR[(4)]': 'a 4-digit year in the range 1901 to 2155',
	'CHAR[(M)]' : 'a M-length string right-padded with spaces; M in [0,255], default = 1',
	'VARCHAR(M)': 'a variable-length string with max length M; M in range [0,65535]',
	'BINARY[(M)]': 'a CHAR[(M)] stored in binary',
	'VARBINARY(M)': 'a VARCHAR(M) stored in binary',
	'TINYBLOB': 'up to 255 bytes',
	'TINYTEXT': 'a string stored as up to 255 bytes',
	'BLOB': 'up to 65535 bytes',
	'TEXT': 'a string stored as up to 65535 bytes',
	'MEDIUMBLOB': 'up to 16777215 bytes',
	'MEDIUMTEXT': 'a string stored as up to 16777215 bytes',
	'LONGBLOB': 'up to 42942967295B = 4GB',
	'LONGTEXT': 'a string stored as up to 42942967295B = 4GB',
	'ENUM(\'value1\',\'value2\',...)': 'one value from a set; max length of a value is 255 char or 1020 bytes',
	'SET(\'value1\',\'value2\',...)': 'a set of up to 64 values; max length of a value is 255 char or 1020 bytes'
}