#!/usr/bin/env python
"""
Implements a thread safe DB connections pool.
"""

__version__ = "$Id$"
__revision__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

import logging
from threading import Condition

##############################################################################

class SafePool:
    """
    Implements a thread safe DB connections pool.
    """

    ##########################################################################

    def __init__(self, dbInstance, maxNonAssignedConnections = 5):
        """
        Initialize a safe pool object
        """

        # store parameters
        self.dbInstance = dbInstance

        # connections
        self.availableConnections = []

        # maximum number of non assigned connections 
        self.maxNonAssignedConnections = maxNonAssignedConnections 

        # create semaphore
        self.lock = Condition()

    ##########################################################################

    def getConnection(self):
        """
        Get the oldest connections from the pool, checking that is
        still valid. If no connections are available, create a new one
        """

        logging.debug("SafePool: DB connection requested from pool")

        # start critical section
        self.lock.acquire()
        
        # verify if there are available connections
        if len(self.availableConnections) != 0:

            # get the oldest connection in pool
            conn = self.availableConnections.pop(0)

            # loop till get a working one from available set
            (working, cursor) = self.checkConnection(conn)

            while not working and len(self.availableConnections) != 0:

                # does not work, try the next one
                conn = self.availableConnections.pop(0)
                (working, cursor) = self.checkConnection(conn)

            # check if a working connection was found
            if working:

                # end critical section
                self.lock.release()

                # return to user
                logging.debug("SafePool: DB connection returned from pool")
                return (conn, cursor)

        # get connection and properties
        (conn, cursor) = self.dbInstance.getConnection()

        # end critical section
        self.lock.release()

        # return working connection and cursor
        logging.debug("SafePool: DB connection created")
        return (conn, cursor)

    ##########################################################################

    def checkConnection(self, conn):
        """
        Return a cursor if the connections is valid, (False, None) in
        other case.
        """

        # set transaction properties implicitely checking connection
        try:
            cursor = self.dbInstance.getCursor(conn)
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET AUTOCOMMIT=0")

        # it does not work
        except self.dbInstance.exception:
            return (False, None)

        # connection is fine, return cursor
        return (True, cursor)

    ##########################################################################

    def releaseConnection(self, conn):
        """
        Insert the released connection into the pool, closing the oldest
        connections if the maximum number of connections has been reached
        """

        # check if it is a connection or a connection object
        if type(conn) is tuple:
            conn = conn[0]

        # start critical section
        self.lock.acquire()

        # add connection to pool
        self.availableConnections.append(conn)

        # verify limit has not been reached
        if len(self.availableConnections) <= self.maxNonAssignedConnections:

            # end critical section and return
            self.lock.release()
            logging.debug("SafePool: connection released to pool")
            return

        # get oldest connection from pool
        conn = self.availableConnections.pop(0)

        # close it connection ignoring errors if any 
        try:
            self.dbInstance.closeConnection(conn)
        except self.dbInstance.exception:
            pass

        # end critical section
        self.lock.release()
        logging.debug("SafePool: excedent connection closed")

    ##########################################################################

    def closeUnusedConnections(self):
        """
        Close all available connections
        """

        # start critical section
        self.lock.acquire()

        # verify limit has not been reached
        if len(self.availableConnections) == 0:

            # no connections
            self.lock.release()
            logging.debug("No connections to release") 
            return

        # close all connections ignoring errors if any
        for conn in self.availableConnections:
            self.dbInstance.closeConnection(conn)
 
        # end critical section
        self.lock.release()
        logging.debug("SafePool: unused connections closed")


