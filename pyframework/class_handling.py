
def make_classes(self):
	"""
	Call to generate a class for each table in db
	Classes are put into the `tables` directory

	Not yet utilized anywhere
	"""
	# open the connection if needed
	# keep track of if we need to close it
	if not self.open:
		raise Exception('You must initiate the connection.')

	# make directory `tables` if needed
	if not os.path.isdir('tables'):
		os.mkdir('tables')

	# get the tables
	tables = self.query('show','tables')
	tables = [Iterable(i.values())[0] for i in tables]

	# write init file for tables module
	with open('tables/__init__.py','w') as f:
		for i in tables:
			f.write(f"from tables.{i}' import {i}\n")

	# write the classes
	for i in tables:
		columns = self.query('describe', i)
		with open(f"tables/{i}.py",'w') as f:
			f.write(f"class {i}:\n")
			f.write('\tdef __init__(self, data):\n')
			for j in columns:
				f.write(f"\t\tself.{j['Field']} = data[\'{j['Field']}\']\n")

			f.write('\t\tself.columns = [' + ', '.join([f"\'{j['Field']}\'" for j in columns]) + ']\n')

			f.write('\n\tdef __repr__(self):\n')
			f.write('\t\treturn str({i: getattr(self,i) for i in self.columns})')

	return self

def remake_class(self, table: Union[str, Table]):
	"""
	Remakes the class for a specific table

	str @name is the table for which to reload the table
	"""
	if not self.open:
		raise Exception('You must initiate the connection.')
	if not isinstance(table, str):
		table = repr(table)

	columns = self.query('describe', table)

	if not os.path.isdir('tables'):
		os.mkdir('tables')
	with open(f"tables/{table}.py",'w') as f:
		f.write(f"class{table}:\n")
		f.write('\tdef __init__(self, data):\n')
		for j in columns:
			f.write(f"\t\tself.{j['Field']} = data[\'{j['Field']}\']\n")

		f.write('\t\tself.columns = [' + ', '.join([f"\'{j['Field']}\'" for j in columns]) + ']\n')

		f.write('\n\tdef __repr__(self):\n')
		f.write('\t\treturn str({i: getattr(self,i) for i in self.columns})')
	
	return self