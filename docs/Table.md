# Table

## __init__(self, ...
1. `db: Any`
   - the database to which this table belongs
2. `name: str`
   - the name of the table itself
3. `columns: Iterable[Column]`
   - the column that belong to this table
   - if column._table exists, sets it to the new table
- `temporary: bool = False`
  - whether or not this a temporary table
- `increment: int = None`
  - the initial value for auto incrementating
- `comment: str = ""`
  - a comment about the table

## Properties
### column_names : List[str]
returns a list of the names of the columns
calculated on the fly as `column.name` or `str(column)` for each column in `self.columns`

## Methods
### Table.add_foreign_key(self, col, foreign, *, name=None)
adds a ForeignKey to between the given local column and the given foreign column

### Table.add_index(self, col, *, name=None, unique=False)
adds an Index to the given local column, optionally unique

### Table.add_key(self, col, *, name=None, **kwargs)
based off presence of `foreign`, `primary`, `unique` kwargs, add the requested key

### Table.add_primary_key(self, col, *, name=None)
adds a PrimaryKey to the given local column

### Table.add_unique(self, col, *, name=None)
adds a unique constraint to the given local column
equivalent to `self.add_index(col, unique=True, name=name)`

### Table.count(self, where=None, groupby=None)
returns the count of the entries

### Table.delete(self, where=None)
deletes the entries

### Table.distinct(self, fields='all', **kwargs)
returns distinct values of the entries across the given fields, or special keyword `all`


### Table.drop_foreign_key(self, foreign_key)
drop the given ForeignKey

### Table.drop_index(self, index, *, unique=False)
drop the given Index, optionally unique

### Table.drop_key(self, col, *, name=None, **kwargs)
based off presence of `foreign`, `primary`, `unique` kwargs, drop the requested key

### Table.drop_primary_key(self, primary_key)
drop the given PrimaryKey

### Table.drop_unique(self, unique_key)
drops the given unique constraint
equivalent to `self.drop_index(unique_key, unique=True)`

### Table.get_column(self, col)
gets the given column gracefully
if `col` in `self.columns`, returns `col` directly
if `col` in `self.column_names`, gets from `self.columns` by index
else returns `None`

### Table.insert(fields)
inserts entries into the given fields

### Table.join(self, table, on, direction='inner', alias=None)
returns a `Table` that allows for querying on the inner/left/right joined with @table aliased as @alias on @on

### Table.select(fields='all', **kwargs)
returns the values from the given fields for the entries

### Table.update(fields, where=None)
update the values in the given fields for the entries


# Column

## __init__(self, ...
1. `table : Table`
   - the table to which this column belongs
2. `name : str `
   - the name of this column
3. `dtype : Any`
   - the type of this column
- `null: bool = False`
  - whether or not the column can take a null value
- `default: Any = None`
  - the default value of the column if not given upon insert
- `visible: bool = True`
  - whether the column is visible or not
- `increment: bool = False`
  - whether or not to auto increment on each insert
- `unique: bool = False`
  - whether or not the column must be unique
- `key: bool = False`
  - whether or not the column is a key aka index
- `primary: bool = False`
  - whether or not the column is the primary key
  - implies @unique and @key are True
- `comment: str = ""`
  - a comment about the column
- `constraints: Iterable[AbstractConstraint] = []`
  - a list of the constraint objects on this column

## __bool__
if it's a comparison, returns whether the two columns are the same
else returns `True`

## __repr__
str representation of self for queries

## __str__
print-friendly str of self to display

## Methods
### Column.from_definition(cls, definition, *, table=None)
creates a Column by parsing SQL column definition string
see docstring for exact syntax

### Column.get_constraint(self, constraint)
if `constraint` is one of `"index"`, `"primary"`, or `"unique"`:
- returns the given constraint or None 
  - primary relation is kept in preference to unique
- if multiple matches, remove all but the returned instance
otherwise, just returns its argument

## Pass-through Methods
route to `self.table.db` functions for operations
delegated further to `self.table.db.translator.[func]()`
Operations:
- `__add__(self, a)`
- `__mul__(self, a)`
- `__sub__(self, a)`
- `__truediv__(self, a)`
Comparators:
- `__and__(self, a)`
- `__contains__(self, a)`
- `__eq__(self, a)`
- `__ge__(self, a)`
- `__gt__(self, a)`
- `__lt__(self, a)`
- `__le__(self, a)`
- `__mod__(self, a)`
- `__ne__(self, a)`
- `__or__(self, a)`

# AbstractConstraint
## __init__(self, ...
1. `target : Column`
   - the column to add this constraint to
   - appends itself to `target._constraints`
- `name : str`
  - the name of this constraint

## Methods
### drop(self)
remove `self` from `self.target._constraints`

### validate(self, value)
implemented by each child class below
returns `self` if `value` is valid, otherwise raises `ValueError`

### validate_and_raise(self)
updates `self.values` with the existing values in the constraint

# SubClasses of AbstractConstraint
## Index
does no checking of given `value` in `validate`

## Unique
ensures no value is repeated

### PrimaryKey(Unique)
no differences from pass `Unique` constraint

### ForeignKey(Unique)
ensures values are in the given `foreign` column
#### __init__(self, ...
1. `target : Column`
   - the column to add this constraint to
   - appends itself to `target._constraints`
2. `foreign: Column`
   - the column that provides the set of allowable values
   - appends itself to `foreign._constraints`
- `name : str`
  - the name of this constraint

