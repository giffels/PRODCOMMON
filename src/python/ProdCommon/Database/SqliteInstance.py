#!/usr/bin/env python
"""
Implements DB object that deals with SQLITE DB connection operations
"""

__version__ = "$Id: SqliteInstance.py,v 1.1 2007/12/05 14:53:14 ckavka Exp $"
__revision__ = "$Revision: 1.1 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from ProdCommon.Database.DbInstance import DbInstance

# Warning: SQLite specifics, not default in PA distribution!
try:
    from pysqlite2 import dbapi2 as sqlite
except:
    import sqlite3 as sqlite 
##############################################################################

class SqliteInstance(DbInstance):
    """
    Implements a DB object that deals with SQLITE DB connection operations
    """

    ##########################################################################

    def __init__(self, dbParams):
        """
        Initialize SQLITE instance object
        """

        # initalize members
        super(SqliteInstance, self).__init__(dbParams)
        self.exception = sqlite.Error

    ##########################################################################

    def getConnection(self):
        """
        get a SQLITE connection
        """

        # create a connection
        conn = sqlite.connect(self.dbParams['dbName'])

        # set transaction properties
        conn.isolation_level = "deferred"

        # get a cursor 
        cursor = self.getCursor(conn)

        # return connection and cursor
        return(conn, cursor)

    ##########################################################################

    def getCursor(self, conn):
        """
        get a SQLITE cursor
        """
        return conn.cursor()

    ##########################################################################

    def closeConnection(self, conn):
        """
        close a SQLTE connection
        """

        # close connection ignoring errors if any
        try:
            conn.close()
        except sqlite.Error:
            pass




