"""
A minimal user management system built on top of the optional pyframework module.
If not using pyframework, check each function for its requirements for attr of @db.
	See module database for further explanation of each Database attr.
The `users` table of the database should have at minimum the columns `username`, `password`, and `email`.


Requirements:
	MySQLdb
	passlib
"""

from MySQLdb import escape_string
from passlib.hash import pbkdf2_sha256
from typing import Any, Dict, Tuple

try:
	from pyframework import Database
except ImportError:
	Database = Any


def get_userID(db: Database, username:str):
	"""
Returns the userID for a given username.
Database @db with attr `open`, `users` with attr `select(fields: List[str], where: str)`
str @username - escaped
	"""
	return db.users.select(['userid'],where='username=\''+escape_string(username).decode()+'\'') if db.open else -1


def login(db: Database, username:str, password:str) -> Tuple[str, dict]:
	"""
Login a user
Returns user as dict or None, error as str
	If no error occured, error is ''. Else it is a description of the error.
	If the username cannot be found, user is ''. Else it is the database dict for the user.

Database @db with attr `users` with attr `select('all', where: str)
str @username - escaped
str @password - cannot escape
	"""
	user = {}
	error = ''
	if not db.open:
		error = 'Database must be opened. Please contact site owner.'
	else:
		user = db.users.select('all',where='username=\''+escape_string(username).decode()+'\'')
		if user:
			error = not pbkdf2_sha256.verify(password,user['password'])
			if error:
				error = 'Incorrect password.'
				user = {}
		else:
			error =  'Username ' + username + ' was not found.'
			user = {}

	return user, error


def signup(db: Database, username: str, password: str, email: str, **kwargs) -> str:
	"""
Adds a new user to the users table of the db.
Returns a str desciprion of the error or '' if no error occurred.

Database @db with attr `users` with attr `insert(Dict[str, Any]) -> str` and attr `col_names() -> List[str]`.
str @username - escaped; checked for uniqueness
str @password - cannot escape
str @email - escaped
Any **kwargs - each value escaped; checked that key in @db.users.col_names()
	"""
	error = ''
	if not db.open:
		error = 'Database must be opened. Please contact site owner.'
	elif get_userID(username):
		error = 'Username already taken. Please try a different name.'

	cols = db.users.col_names()
	for i in kwargs:
		if not i in cols:
			raise Exception('Column '+i+' not in the `users` table of @db.')
	
	if not error:
		to_insert = {
				'username': escape_string(username).decode(),
				'email': escape_string(email).decode(),
				'password': pbkdf2_sha256.hash(password) # can't escape this
			}
		to_insert.update({k: escape_string(v).decode() for k,v in kwargs.items()})

		error = db.users.insert(to_insert)
		db.commit()

	return error