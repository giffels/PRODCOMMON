#!/usr/bin/env python
"""
Implements a DB session which:
    * is thread safe.
    * has redo transaction capabilities.
    * supports connection pool.
    * allows subclassing.
    * deals with different database types.
"""

__version__ = "$Id: SafeSession.py,v 1.2 2007/12/07 10:19:26 ckavka Exp $"
__revision__ = "$Revision: 1.2 $"
__author__ = "Carlos.Kavka@ts.infn.it"

##############################################################################

class SafeSession(object):
    """
    Implements a DB session which:
    * is thread safe.
    * has redo transaction capabilities. 
    * supports connection pool.
    * allows subclassing.
    * deals with different database types.
    """

    ##########################################################################

    def __init__(self, pool = None, dbInstance = None):
        """
        Pool or dbInstance can be specified but not both
        """

        # check pool and DB parameters
        if (pool is not None and dbInstance is not None) or \
           (pool is None and dbInstance is None):
            raise ValueError("wrong parameters when creating SafeSession")

        # store parameters and open a connection
        self.dbInstance = dbInstance
        self.pool = pool
        self.conn = None
        self.cursor = None

        # define an exception
        if self.dbInstance is not None:
            self.exception = self.exception
        else :
            self.exception = self.pool.dbInstance.exception

        # get a connection
        self.connect()

        # set current transaction
        self.transaction = []

    ##########################################################################

    def connect(self):
        """
        Creates a connection
        """

        # get a connection
        if self.pool is None:
            (self.conn, self.cursor) = self.dbInstance.getConnection()
        else:
            (self.conn, self.cursor) = self.pool.getConnection()

    ##########################################################################

    def commit(self):
        """
        Commit changes into DB
        """

        # commit
        try:
            self.conn.commit()

        except self.exception:

            # lost connection with database, reopen it
            self.redo()

            # try to commit
            self.conn.commit()

        # erase redo list
        self.transaction = []

    ##########################################################################

    def startTransaction(self):
        """
        Start a transaction
        """

        # perform implicit commit if necessary
        if self.transaction != []:

            # commit
            try:
                self.conn.commit()

            except self.exception:

                # lost connection with database, reopen it
                self.redo()

                # try to commit
                self.conn.commit()

        # erase redo list
        self.transaction = []

    ##########################################################################

    def rollback(self):
        """
        Rollback a transaction
        """

        # roll back
        try:
            self.conn.rollback()

        except self.exception:
            # lost connection con database, just get a new connection
            # the effect of rollback is then automatic

            # refresh connection
            self.connect()

        # erase redo list
        self.transaction = []

    ##########################################################################

    def redo(self):
        """
        Redo all operations from a transaction
        """

        # force a connection reopen
        self.connect()
 
        # perform all operations in current newly created transaction
        for sqlOperation in self.transaction:
            self.cursor.execute(sqlOperation)

    ##########################################################################

    def execute(self, query):
        """
        Execute a query
        """

        # execute query
        try:
            self.cursor.execute(query)
            rows = self.cursor.rowcount

        # lost connection with database, reopen it, redo current
        # transaction and retry query
        except self.exception:
            self.redo()
            self.cursor.execute(query)
            rows = self.cursor.rowcount

        # add it to transaction redo list
        self.transaction.append(query)

        # return rows counter
        return rows

    ##########################################################################

    def fetchall(self):
        """
        Get all rows from previous query
        """

        # execute query
        try:
            results = self.cursor.fetchall()

        # lost connection with database, reopen it, redo current
        # transaction and retry query
        except self.exception:
            self.redo()
            results = self.cursor.fetchall()

        # return results
        return results

    ##########################################################################

    def fetchone(self):
        """
        Get single row from previous query
        """

        # execute query
        try:
            results = self.cursor.fetchone()

        # lost connection with database, reopen it, redo current
        # transaction and retry query
        except self.exception:
            self.redo()
            results = self.cursor.fetchone()

        # return results
        return results

    ##########################################################################

    def close(self):
        """
        Close a connection or return it to pool
        """

        # perform an implicit commit
        self.commit()

        # close or release connection
        if self.pool is None:
            self.dbInstance.closeConnection(self.conn)
        else:
            self.pool.releaseConnection(self.conn)


