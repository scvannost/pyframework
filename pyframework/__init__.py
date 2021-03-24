"""
A set of layered models that implement many forms of database-like behavior using object-oriented programming

AbstractDatabase, and AbstractTranslator are fully duck-typed

Column, Table are based off the MySQL schemas
All other classes (including other abstract classes) are built on top of these

Class inheritance structure
---------------------------
AbstractDatabase			  - AbstractTranslator
	AbstractSqlDatabase 	  -		AbstractSqlTranslator			<- Table/Column dependency begins
		AbstractMySqlDatabase - 		AbstractMySqlTranslator
			MysqldbDatabase	  - 			MysqldbTranslator		<- requires MySQLdb
"""
from .AbstractDatabase import AbstractDatabase, AbstractTranslator
from .Table import Column, Table

from .AbstractDatabase import AbstractSqlDatabase, AbstractSqlTranslator
from .AbstractMySqlDatabase import AbstractMySqlDatabase, AbstractMySqlTranslator

from .MysqldbDatabase import MysqldbDatabase, MysqldbTranslator
