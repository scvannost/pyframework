# MysqldbDatabase

## __init__(self, ...
1. `location : str`
   - the location of the server
2. `user : str`
   - the username
3. `password : str`
   - the password for @user
4. `name : str`
   - the name of the database to use
- `Translator : Type[AbstractTranslator] = MysqldbTranslator`
   - the translator type for this database
   - `self.translator` is set to `Translator(self)`

## Properties

### db : Any
the actual database object
initialized as `None`

### open : bool
whether or not the database connection is open
will check `self.db.open` if `self.db is not None`

### tables : List[Table]
a Table object for each table in the database
initialized as `[]`

### table_names List[str]
an list of the names of all the tables
calculated on the fly as `table.name` or `str(table)` for each table in `self.tables`

### translator : AbstractTranslator
a Translator for the particular syntax of the database object

## Methods
### AbstractDatabase.close(self)
closes the connection to the database if `self.open`
call `self.db.close()`, reset `self.tables`, garbage collect

### MysqldbDatabase.connect(self)
connects to the database using `MySQLdb.connect()`
also fills in `self.tables` using `self.query("show", "tables")`

### AbstractSqlDatabase.commit(self)
commits changes to the database, returning self
errors if not `self.open`

### MysqldbDatabase.db_query(self, q, **kwargs)
queries the database for MySQL query string `q`, see `MySQLdb` for full kwargs
returns `None` if no result eg DELETE, UPDATE

### AbstractDatabase.get_column(self, table, column)
gets the given column gracefully
uses `self.get_table(table).set_column(column)`

### AbstractDatabase.get_table(self, table)
gets the given table gracefully
if `table` in `self.tables` or `table.db == self`, returns `table` directly
if `table` in `self.table_names`, gets from `self.tables` by index
else returns `None`

### MysqldbTranslator.interpret(self, results, *args, **kwargs)
takes the response from `self.db_query` and turns it into the desired python types

### AbstractDatabase.is_valid_column(self, table, column, find_table = True)
checks if `column` would be valid on `table`
pass `find_table=False` if `self.get_table(table)` would return `None`
#### AbstractTranslator.is_valid_column(self, table, column)
checks `column` has valid `dtype` and that doesn't exist on `table`

### AbstractSqlDatabase.query(self, method, table, fields, extra, kwargs)
provides API for querying the database using `self.prepare` and `self.translate` and `self.interpret`

### AbstractSqlDatabase.rollback(self)
rolls back changes to the database, returning self
errors if not `self.open`

### AbstractDatabase.reconnect(self)
same as calling `close()` then `open()`, returning self

### AbstractMySqlTranslator.translate(self, *args, **kwargs)
translates a valid call into a MySQL query string
see docstring for what exactly is handled

### AbstractTranslator.validate(self, *args, **kwargs)
returns `True` if call would not throw an syntax error
uses `self.validate_and_raise`

### AbstractMySqlTranslator.validate_and_raise(self, *args, **kwargs)
raises `Exception`s for syntax errors for a call to pass to `self.translate`
enforces all validations for MySQL calls

## Additional Methods
### AbstractSqlDatabase.add_column(self, table, new, after=None)
add the given column to the given table, returning the modified table
if `after` given, creates the new column after that one. special value `first`

### AbstractSqlDatabase.alter_column(self, table, old, new)
alters the column in the given table that matches `old` to instead match `new`, returning the modified table

### AbstractSqlDatabase.drop_column(self, table, column)
drop the given column from the given table, returning the modified table

### AbstractSqlDatabase.drop_table(self, table, temporary=False)
drop the given table, returning self
set `temporary=True` to only drop TEMPORARY tables

### AbstractSqlDatabase.make_table(self, table, columns, temporary=False, clobber=False)
makes a new table with the given name and columns, returning the new table
set `temporary=True` to create a TEMPORARY table
set `clobber=True` to overwrite existing table1

### AbstractSqlDatabase.move_table(self, old, new)
renames the given table to the new name, returning the modified table

### AbstractSqlDatabase.truncate_table(self, table)
truncates the given table, returning self

## Passthrough Methods - route to self.translator.[func]()
- Operations:
  - `AbstractSqlTranslator.add(self, a, b)`
  - `AbstractSqlTranslator.div(self, a, b)`
  - `AbstractSqlTranslator.mul(self, a, b)`
  - `AbstractSqlTranslator.sub(self, a, b)`
- Comparators:
  - `AbstractSqlTranslator.contains(self, a, b)`
  - `AbstractSqlTranslator.eq(self, a, b)`
  - `AbstractSqlTranslator.ge(self, a, b)`
  - `AbstractSqlTranslator.gt(self, a, b)`
  - `AbstractSqlTranslator.like(self, a, b)`
  - `AbstractSqlTranslator.logical_and(self, a, b)`
  - `AbstractSqlTranslator.logical_or(self, a, b)`
  - `AbstractSqlTranslator.lt(self, a, b)`
  - `AbstractSqlTranslator.le(self, a, b)`
  - `AbstractSqlTranslator.ne(self, a, b)`
    