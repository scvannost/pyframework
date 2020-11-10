"""
A set of layered models that implement many forms of database-like behavior using object-oriented programming

Column, Table, AbstractDatabase, and AbstractTranslator are fully duck-typed
All other classes (including other abstract classes) are built on top of these

Class inheritance structure
---------------------------
AbstractDatabase			  - AbstractTranslator
	AbstractSqlDatabase 	  -		AbstractMySqlTranslator			<- Table/Column dependency begins
		AbstractMySqlDatabase - 		AbstractMySqlTranslator
			MysqldbDatabase	  - 			MysqldbTranslator
"""
from .AbstractDatabase import AbstractDatabase, AbstractTranslator
from .Table import Column, Table

from .AbstractDatabase import AbstractSqlDatabase, AbstractSqlTranslator
from .AbstractMySqlDatabase import AbstractMySqlDatabase, AbstractMySqlTranslator
from .MysqldbDatabase import MysqldbDatabase, MysqldbTranslator
