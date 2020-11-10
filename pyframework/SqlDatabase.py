from AbstractDatabase import *


class SqlDatabse(AbstractDatabase):
	def query(self, method: str, tbl: str, fields: Union[None, Dict[str, Any], List[str], str] = None, extra: str = None, **kwargs):
		"""
		str @method is the SQL method argument. Use 'distinct' for `select distinct`
		str @tbl is the table name
		@fields
			Not required for method = 'count', 'delete'
			Must be a dict of {field: value} for method = 'insert', 'update'
			Must be a list(str) or str or 'all' otherwise.
		str @extra is tacked on the end of the SQL query. This includes `where`, `order by`, etc
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
				raise Exception('Invalid SQL command')

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
				sql += '\'' + self.escape_string(i) + '\','
			sql = sql[:-1]+')'

		elif method == 'update':
			sql += tbl + ' SET '
			for f,v in fields.items():
				sql += f + '=\'' + self.escape_string(v) + '\','
			sql= sql[:-1]

		# append the extra clauses
		if extra:
			sql += ' ' + extra
		sql += ';'

		# call sql_query to get results
		return self.sql_query(sql, **kwargs)