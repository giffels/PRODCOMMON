#!/usr/bin/env python
"""
Implements an abstract DB object that deals with DB connection operations
"""

__version__ = "$Id$"
__revision__ = "$Revision$"
__author__ = "Carlos.Kavka@ts.infn.it"

##############################################################################

class DbInstance(object):
    """
    _DbInstance_

    Implements an abstract DB object that deals with DB connection operations
    """

    # instance variables
    dbParams = None                   # DB parameters
    exception = None                  # DB exception type

    ##########################################################################

    def __init__(self, dbParams):
        """
        Initialize connection object
        """

        self.dbParams = dbParams

    ##########################################################################

    def getConnection(self):
        """
        get a connection
        """

        raise NotImplementedError

    ##########################################################################

    def getCursor(self, conn):
        """
        get a MySQL cursor
        """
        raise NotImplementedError

    ##########################################################################

    def closeConnection(self, conn):
        """
        close a connection
        """

        raise NotImplementedError

    ##########################################################################

    def getExceptionType(self):
        """
        return exception type
        """

        return self.exception


