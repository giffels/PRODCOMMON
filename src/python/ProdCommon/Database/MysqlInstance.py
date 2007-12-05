#!/usr/bin/env python
"""
Implements DB object that deals with MySQL DB connection operations
"""

__version__ = "$Id$"
__revision__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

from ProdCommon.Database.DbInstance import DbInstance

# MySQL specifics
import MySQLdb
from ProdCommon.Database import Mysql

##############################################################################

class MysqlInstance(DbInstance):
    """
    Implements a DB object that deals with MySQL DB connection operations
    """

    ##########################################################################

    def __init__(self, dbParams):
        """
        Initialize MySQL instance object
        """

        # initalize members
        super(MysqlInstance, self).__init__(dbParams)
        self.exception = MySQLdb.Error

    ##########################################################################

    def getConnection(self):
        """
        get a MySQL connection
        """

        # create a connection
        conn = Mysql.connect(self.dbParams['dbName'], \
                             self.dbParams['host'], \
                             self.dbParams['user'], \
                             self.dbParams['passwd'], \
                             self.dbParams['socketFileLocation'],\
                             self.dbParams['portNr'])

        # set transaction properties
        cursor = self.getCursor(conn)
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute("SET AUTOCOMMIT=0")

        # return connection and cursor
        return(conn, cursor)

    ##########################################################################

    def getCursor(self, conn):
        """
        get a MySQL cursor
        """
        return conn.cursor()

    ##########################################################################

    def closeConnection(self, conn):
        """
        close a MySQL connection
        """

        # close connection ignoring errors if any
        try:
            conn.close()
        except MySQLdb.Error:
            pass




